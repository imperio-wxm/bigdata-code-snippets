package com.wxmimperio.hadoop.Deduplication.utils;

import com.wxmimperio.hadoop.Deduplication.connection.HiveConnectionPool;
import com.zaxxer.hikari.HikariDataSource;
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

    private static HiveConnectionPool prestoConnectionPool = HiveConnectionPool.getInstance();
    private static HikariDataSource pool = prestoConnectionPool.getHikariDataSource();
    private static Map<String, TableFormat> formatMapper = new HashMap<>();
    private static String EMPTY = "";
    private static final ThreadLocal<SimpleDateFormat> eventTomeFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

    static {
        try {
            formatMapper.put("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", TableFormat.ORC);
            formatMapper.put("org.apache.hadoop.mapred.SequenceFileInputFormat", TableFormat.SEQUENCEFILE);
            formatMapper.put("org.apache.hadoop.mapred.TextInputFormat", TableFormat.TEXTFILE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static TableFormat checkStorageFormat(String db, String table) throws Exception {
        try (Connection connection = pool.getConnection()) {
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
        try (Connection connection = pool.getConnection()) {
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
        try (Connection connection = pool.getConnection()) {
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
            if (null == val || "\\N".equalsIgnoreCase(val) || "null".equalsIgnoreCase(val)) {
                wc = null;
            } else if (val.isEmpty()) {
                wc = new Text(val);
            } else {
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
            }
        } catch (NumberFormatException e) {
            wc = null;
        }
        oi.setStructFieldData(orcStruct, sf, wc);
    }

    public static String getTempPath(String tableName, String partDate, String fileLocation) throws ParseException {
        return fileLocation +
                tableName +
                "/orc_temp" +
                "/part_date=" +
                partDate +
                "/";
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
        calendar.add(Calendar.SECOND, Math.negateExact(Integer.parseInt(step)));
        return String.valueOf(calendar.getTime().getTime());
    }

    public static long getTimestamp(String time) throws ParseException {
        return HiveUtil.eventTomeFormat.get().parse(time).getTime();
    }

    public static List<String> getDeleteFileName(String tableName, String startTimestamp, String endTimestamp) throws ParseException {
        List<String> fileNameList = new ArrayList<String>();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(Long.parseLong(startTimestamp)));
        long startTime = calendar.getTime().getTime();
        calendar.setTime(new Date(Long.parseLong(endTimestamp)));
        long endTime = calendar.getTime().getTime();
        long diff = (endTime - startTime);
        long nd = 1000 * 24 * 60 * 60;
        long nh = 1000 * 60 * 60;
        long hours = diff % nd / nh + (diff / nd) * 24;
        Calendar startCalendar = Calendar.getInstance();
        startCalendar.setTime(new Date(startTime));
        for (int i = 0; i < hours; i++) {
            String hour = HiveUtil.addZero(startCalendar.get(Calendar.HOUR_OF_DAY) + i, 2);
            if (Integer.parseInt(hour) > 23) {
                break;
            }
            fileNameList.add(tableName + "_" + hour);
        }
        return fileNameList;
    }

    /*CDH4*/
  /*  public static JsonObject convertResultToJson(Result result) {
        if (result == null || result.isEmpty()) {
            return new JsonObject();
        }
        JsonObject data = new JsonObject();
        for (KeyValue kv : result.list()) {
            data.addProperty(Bytes.toString(kv.getQualifier()), Bytes.toString(kv.getValue()));
        }
        return data;
    }*/
}
