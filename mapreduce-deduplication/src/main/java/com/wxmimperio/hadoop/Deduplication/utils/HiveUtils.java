package com.wxmimperio.hadoop.Deduplication.utils;

import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfoFromTypeString;

public class HiveUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HiveUtils.class);

    private static String url;
    private static String username;

    static {
        try {
            String hiveDriver = "org.apache.hive.jdbc.HiveDriver";
            Class.forName(hiveDriver);
            Properties properties = new Properties();
            properties.load(HiveUtils.class.getClassLoader().getResourceAsStream("config.properties"));
            username = properties.getProperty("hive.jdbc.connection.username");
            url = properties.getProperty("hive.jdbc.connection.url");
        } catch (ClassNotFoundException e) {
            LOG.error("HiveDriver not found!", e);
        } catch (IOException e) {
            LOG.error("Get config error!", e);
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
        private String columnName;
        private TypeInfo typeInfo;

        ColumnTypeDesc(String columnName, TypeInfo typeInfo) {
            this.columnName = columnName;
            this.typeInfo = typeInfo;
        }
    }
}
