package com.wxmimperio.hadoop.Deduplication.utils;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wxmimperio.hadoop.Deduplication.connection.HiveConnectionPool;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfoFromTypeString;

public class HiveUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HiveUtils.class);

    private static HiveConnectionPool prestoConnectionPool = HiveConnectionPool.getInstance();
    private static HikariDataSource pool = prestoConnectionPool.getHikariDataSource();
    private static ResourceBundle bundle;

    static {
        bundle = ResourceBundle.getBundle("config", Locale.ENGLISH);
    }

    private static JsonObject getConfigTopics(String url) throws Exception {
        CloseableHttpClient client = HttpClientUtil.getHttpClient();
        String res = HttpClientUtil.doGet(url);
        JsonObject jsonObject = new JsonParser().parse(res).getAsJsonObject();
        jsonObject = jsonObject.get("propertySources")
                .getAsJsonArray()
                .get(0)
                .getAsJsonObject()
                .get("source")
                .getAsJsonObject();

        client.close();
        return jsonObject;
    }

    public static List<String> getConfigTables() {
        List<String> tableNames = Lists.newArrayList();
        try {
            tableNames = combineConfig();
        } catch (Exception e) {
            int reTry = 1;
            while (reTry <= 3) {
                try {
                    Thread.sleep(5000);
                    tableNames = combineConfig();
                    LOG.info("ReTry get hdfs config, times = " + reTry);
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
                reTry++;
                if (CollectionUtils.isNotEmpty(tableNames)) {
                    break;
                }
            }
            if (reTry == 3 && CollectionUtils.isEmpty(tableNames)) {
                throw new RuntimeException("Can not get config topic!");
            }
        }
        return tableNames;
    }

    private static List<String> combineConfig() throws Exception {
        List<String> tableNames = Lists.newArrayList();
        JsonObject jsonObject = getConfigTopics(bundle.getString("hdfs.config.url"));
        JsonObject logObject = getConfigTopics(bundle.getString("hdfs.log.config.url"));
        String schemaTables = jsonObject.get("schema.topic.name") == null ? null : jsonObject.get("schema.topic.name").getAsString();
        String directTables = jsonObject.get("direct.topic.name") == null ? null : jsonObject.get("direct.topic.name").getAsString();
        String logTables = logObject.get("schema.topic.name") == null ? null : logObject.get("schema.topic.name").getAsString();
        tableNames.addAll(StringUtils.isEmpty(schemaTables) ? Lists.newArrayList() : Arrays.asList(schemaTables.split(",", -1)));
        tableNames.addAll(StringUtils.isEmpty(directTables) ? Lists.newArrayList() : Arrays.asList(directTables.split(",", -1)));
        tableNames.addAll(StringUtils.isEmpty(logTables) ? Lists.newArrayList() : Arrays.asList(logTables.split(",", -1)));
        return tableNames;
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
        String showPartition = "SHOW PARTITIONS " + db + "." + tableName + " PARTITION(part_date='" + date + "')";
        String addPartitionHql = "ALTER TABLE " + db + "." + tableName + " ADD PARTITION(part_date='" + date + "')";
        String partitionSearch = null;
        try (Connection connection = pool.getConnection()) {
            Statement statement = connection.createStatement();
            try {
                ResultSet resultSet = statement.executeQuery(showPartition);
                while (resultSet.next()) {
                    partitionSearch = resultSet.getString(1);
                }
                if (StringUtils.isEmpty(partitionSearch)) {
                    statement.execute(addPartitionHql);
                    LOG.info("Table " + tableName + " partition=" + date + " add.");
                } else {
                    LOG.warn("Table " + tableName + " partition='" + date + "' already exists.");
                }
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
        private String columnName;
        private TypeInfo typeInfo;

        ColumnTypeDesc(String columnName, TypeInfo typeInfo) {
            this.columnName = columnName;
            this.typeInfo = typeInfo;
        }
    }

    public static void closeHiveConnect() {
        prestoConnectionPool.closePool();
    }
}
