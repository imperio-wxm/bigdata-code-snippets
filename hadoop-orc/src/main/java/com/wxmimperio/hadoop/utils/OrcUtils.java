package com.wxmimperio.hadoop.utils;

import com.wxmimperio.hadoop.pojo.HiveColumnTypeDesc;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class OrcUtils {
    private static final Logger LOG = LoggerFactory.getLogger(OrcUtils.class);

    private static String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static String url;
    private static String username;

    static {
        try {
            Class.forName(HIVE_DRIVER);
            Properties properties = new Properties();
            properties.load(OrcUtils.class.getClassLoader().getResourceAsStream("config.properties"));
            username = properties.getProperty("hive.jdbc.connection.username");
            url = properties.getProperty("hive.jdbc.connection.url");
        } catch (ClassNotFoundException e) {
            LOG.error("HiveDriver not found!", e);
        } catch (IOException e) {
            LOG.error("Get config error!", e);
        }
    }

    /**
     * Jdbc for hive table and get partition key list
     *
     * @param db
     * @param table
     * @param connection
     * @return
     * @throws Exception
     */
    public static List<HiveColumnTypeDesc> getPartitionColumns(String db, String table, Connection connection) throws Exception {
        List<HiveColumnTypeDesc> ctdList = new ArrayList<>();
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
                    ctdList.add(new HiveColumnTypeDesc(partKey, TypeDescription.fromString(partType)));
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

    /**
     * Get orcFile schema
     *
     * @param db
     * @param tableName
     * @return
     * @throws Exception
     */
    public static TypeDescription getColumnTypeDescs(String db, String tableName) throws Exception {
        TypeDescription schema = TypeDescription.createStruct();
        // jdbc to hive for table info
        try (Connection connection = DriverManager.getConnection(url, username, "")) {
            List<HiveColumnTypeDesc> ctdList = getPartitionColumns(db, tableName, connection);
            Set<String> partCol = new HashSet<>();
            for (HiveColumnTypeDesc ctd : ctdList) {
                partCol.add(ctd.columnName.toLowerCase());
            }
            ResultSet res = connection.getMetaData().getColumns(db, db, tableName, "%");
            String columnName, typeName;
            while (res.next()) {
                columnName = res.getString("COLUMN_NAME");
                typeName = res.getString("TYPE_NAME");
                // partition key not int schema
                if (!partCol.contains(columnName.toLowerCase())) {
                    schema.addField(columnName, TypeDescription.fromString(typeName));
                }
            }
        }
        return schema;
    }
}
