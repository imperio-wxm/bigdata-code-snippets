package com.wxmimperio.hbase.utils;

import com.google.gson.JsonObject;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfoFromTypeString;

public class HiveUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HiveUtil.class);

    public enum TableFormat {
        UNKNOWN, TEXTFILE, SEQUENCEFILE, ORC
    }

    private static Map<String, TableFormat> formatMapper = new HashMap<>();
    private static String username;
    private static String url;
    public static String EMPTY = new String("");
    public static final ThreadLocal<SimpleDateFormat> eventTomeFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    static {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            Properties properties = new Properties();
            properties.load(HiveUtil.class.getClassLoader().getResourceAsStream("config.properties"));
            username = properties.getProperty("hive.jdbc.connection.username");
            url = properties.getProperty("hive.jdbc.connection.url");
            formatMapper.put("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", TableFormat.ORC);
            formatMapper.put("org.apache.hadoop.mapred.SequenceFileInputFormat", TableFormat.SEQUENCEFILE);
            formatMapper.put("org.apache.hadoop.mapred.TextInputFormat", TableFormat.TEXTFILE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static TableFormat checkStorageFormat(String db, String table) throws Exception {
        try (Connection connection = DriverManager.getConnection(url, username, "")) {
            Statement statement = connection.createStatement();
            ResultSet res = statement.executeQuery(String.format("describe formatted %s.%s", db, table));
            String col1 = null;
            while (res.next()) {
                col1 = res.getString(1);
                if (null != col1 && "InputFormat:".equals(col1.trim())) {
                    return formatMapper.get(res.getString(2).trim());
                }
            }
            return TableFormat.UNKNOWN;
        }
    }

    private static List<ColumnTypeDesc> getPartitionColumns(String db, String table, Connection connection) throws Exception {
        List<ColumnTypeDesc> ctdList = new ArrayList<>();
        Statement statement = connection.createStatement();
        ResultSet res = statement.executeQuery(String.format("describe %s.%s", db, table));
        String title = null;
        String partKey = null;
        String partType = null;
        boolean findPartition = false;
        while (res.next()) {
            if (findPartition) {
                partKey = res.getString(1);
                if (null != partKey && !partKey.startsWith("# ") && !"".equals(partKey.trim())) {
                    partKey = partKey.trim();
                    partType = res.getString(2).trim();
                    ctdList.add(new ColumnTypeDesc(partKey, getTypeInfoFromTypeString(partType)));
                    LOG.info("find partition key:" + partKey + " " + partType);
                }
            } else {
                title = res.getString(1);
                if (null != title && "# Partition Information".equals(title.trim())) {
                    findPartition = true;
                }
            }
        }
        return ctdList;
    }

    public static void addPartition(String db, String tableName, String date) throws SQLException {
        String addPartitionHql = "ALTER TABLE " + db + "." + tableName + " ADD PARTITION(part_date='" + date + "')";
        try (Connection connection = DriverManager.getConnection(url, username, "")) {
            Statement statement = connection.createStatement();
            try {
                statement.execute(addPartitionHql);
                LOG.info("Table " + tableName + " partition=" + date + " add.");
            } catch (SQLException e) {
                if (e.getMessage().trim().contains("AlreadyExistsException")) {
                    LOG.info("Table " + tableName + " partition=" + date + " already exists.");
                } else {
                    throw new SQLException(e);
                }
            }
        }
    }

    public static StructTypeInfo getColumnTypeDescs(String db, String table) throws Exception {
        Map<String, TypeInfo> typeInfoMap = new LinkedHashMap<>();
        try (Connection connection = DriverManager.getConnection(url, username, "")) {
            List<ColumnTypeDesc> ctdList = getPartitionColumns(db, table, connection);
            Set<String> partCol = new HashSet<>();
            for (ColumnTypeDesc ctd : ctdList) {
                partCol.add(ctd.columnName.toLowerCase());
            }
            ResultSet res = connection.getMetaData().getColumns(db, db, table, "%");
            String columnName, typeName;
            LOG.info("Read schema for " + db + "." + table);
            while (res.next()) {
                columnName = res.getString("COLUMN_NAME");
                typeName = res.getString("TYPE_NAME");
                if (!partCol.contains(columnName.toLowerCase())) {
                    LOG.info("Read column schema " + columnName + ":" + typeName);
                    typeInfoMap.put(columnName, getTypeInfoFromTypeString(typeName.toLowerCase()));
                }
            }
        }
        StructTypeInfo typeInfo = (StructTypeInfo) getTypeInfoFromTypeString("struct<>");
        typeInfo.setAllStructFieldNames(new ArrayList<>(typeInfoMap.keySet()));
        typeInfo.setAllStructFieldTypeInfos(new ArrayList<>(typeInfoMap.values()));
        return typeInfo;
    }

    public static class ColumnTypeDesc {
        public String columnName;
        public TypeInfo typeInfo;

        public ColumnTypeDesc(String columnName, TypeInfo typeInfo) {
            this.columnName = columnName;
            this.typeInfo = typeInfo;
        }
    }

    public static void formatFieldValue(SettableStructObjectInspector oi, StructField sf, OrcStruct orcStruct, String val) {
        WritableComparable wc = null;
        try {
            switch (sf.getFieldObjectInspector().getTypeName().toLowerCase()) {
                case "string":
                case "varchar":
                    wc = new Text(val);
                    break;
                case "bigint":
                    wc = new LongWritable(Long.valueOf(val));
                    break;
                case "int":
                    wc = new IntWritable(Integer.valueOf(val));
                    break;
                case "boolean":
                    wc = new BooleanWritable(Boolean.valueOf(val));
                    break;
                case "smallint":
                    wc = new ShortWritable(Short.valueOf(val));
                    break;
                case "float":
                    wc = new FloatWritable(Float.valueOf(val));
                    break;
                case "double":
                    wc = new DoubleWritable(Double.valueOf(val));
                    break;
                default:
                    break;
            }
        } catch (NumberFormatException e) {
            wc = null;
        }
        oi.setStructFieldData(orcStruct, sf, wc);
    }

    public static String getTempPath(String tableName, String partDate, String fileLocation) throws ParseException {
        StringBuilder filePath = new StringBuilder();
        filePath.append(fileLocation)
                .append(tableName)
                .append("/orc_temp")
                .append("/part_date=")
                .append(partDate)
                .append("/");
        return filePath.toString();
    }


    public static String getRealPath(String tempPath, String fileName) {
        return tempPath.replaceAll("/orc_temp", EMPTY) + fileName;
    }

    /**
     * get file name, like test_table_11_12_1513664756483
     *
     * @param tableName
     * @param startTimestamp
     * @param endTimestamp
     * @return
     * @throws ParseException
     */
    public static String getFileName(String tableName, String startTimestamp, String endTimestamp) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(Long.parseLong(startTimestamp)));
        String startHour = addZero(calendar.get(Calendar.HOUR_OF_DAY), 2);
        calendar.setTime(eventTomeFormat.get().parse(endTimestamp));
        String endHour = addZero(calendar.get(Calendar.HOUR_OF_DAY), 2);
        return tableName + "_" + startHour + "_" + endHour + "_" + System.currentTimeMillis();
    }

    public static String addZero(int num, int len) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(num);
        while (stringBuilder.length() < len) {
            stringBuilder.insert(0, "0");
        }
        return stringBuilder.toString();
    }

    /**
     * get start timestamp by endTimestamp and step
     *
     * @param endTimestamp
     * @param step
     * @return
     * @throws ParseException
     */
    public static String getStartTimestamp(String endTimestamp, String step) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(HiveUtil.eventTomeFormat.get().parse(endTimestamp));
        calendar.add(Calendar.SECOND, Integer.parseInt(step));
        return String.valueOf(calendar.getTime().getTime());
    }

    public static long getTimestamp(String time) throws ParseException {
        return HiveUtil.eventTomeFormat.get().parse(time).getTime();
    }

    public static List<String> getDeleteFileName(String tableName, String startTimestamp, String endTimestamp) throws ParseException {
        List<String> fileNameList = new ArrayList<String>();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(Long.parseLong(startTimestamp)));
        int startHour = calendar.get(Calendar.HOUR_OF_DAY);
        calendar.setTime(new Date(Long.parseLong(endTimestamp)));
        int endHour = calendar.get(Calendar.HOUR_OF_DAY);
        for (int i = 0; i < (endHour - startHour); i++) {
            fileNameList.add(tableName + "_" + HiveUtil.addZero(startHour + i, 2));
        }
        return fileNameList;
    }

    public static JsonObject convertResultToJson(Result value) {
        JsonObject jsonObject = new JsonObject();
        for (Cell cell : value.rawCells()) {
            jsonObject.addProperty("Rowkey", new String(CellUtil.cloneRow(cell)));
            jsonObject.addProperty(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
        }
        return jsonObject;
    }


    public static void main(String[] args) throws Exception {
       /* TableFormat tf = checkStorageFormat("suheng", "tbl_core_raw");
        System.out.println(tf);
        getColumnTypeDescs("suheng", "mobile_app_reg_mid");*/
//        StructTypeInfo typeInfo = (StructTypeInfo)getTypeInfoFromTypeString("struct<>");

        System.out.println(getDeleteFileName("test_table_1214", "1513651854000", "1513655454000"));

        System.out.println(getStartTimestamp("2017-12-21 10:18:47", "-60"));

        //addPartition("dw", "test_table_1214", "2017-12-18 ");
    }
}
