package com.wxmimperio.hadoop.utils;

import com.wxmimperio.hadoop.pojo.HiveColumnTypeDesc;
import org.apache.orc.TypeDescription;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class OrcUtils {
    private static String url;
    private static String username;

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
            ResultSet res = connection.getMetaData().getColumns(db, db, tableName, "%");
            String columnName, typeName;
            while (res.next()) {
                columnName = res.getString("COLUMN_NAME");
                typeName = res.getString("TYPE_NAME");
                schema.addField(columnName, TypeDescription.fromString(typeName));
            }
        }
        return schema;
    }
}
