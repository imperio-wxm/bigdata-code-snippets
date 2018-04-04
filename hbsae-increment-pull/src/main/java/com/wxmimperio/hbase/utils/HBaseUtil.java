package com.wxmimperio.hbase.utils;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class HBaseUtil {

    private static String HBASE_SITE = "hbaes-site.xml";
    private static Connection connection;

    private synchronized static Connection getConnection() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(HBASE_SITE);
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        if (connection == null) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    public static JsonObject getResult(String rowKey, String tableName) throws IOException {
        JsonObject jsonObject = new JsonObject();
        try (Table table = getConnection().getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            List<Cell> cs = result.listCells();
            if (null == cs || cs.size() == 0) {
                return jsonObject;
            }
            for (Cell cell : cs) {
                jsonObject.addProperty("rowKey", Bytes.toString(CellUtil.cloneRow(cell)));// 取行健
                jsonObject.addProperty(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
            }
        }
        return jsonObject;
    }
}
