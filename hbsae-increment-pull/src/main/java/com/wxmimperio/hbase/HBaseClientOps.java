package com.wxmimperio.hbase;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class HBaseClientOps implements Serializable {
    private static Logger LOG = LoggerFactory.getLogger(HBaseClientOps.class);

    private static final long serialVersionUID = 1L;
    private static String HBASE_SITE = "hbaes-site.xml";
    private static Connection connection;

    static {
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
        LOG.info("HBase client build success!");
    }

    public HBaseClientOps() {
    }

/*    public void initConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(HBASE_SITE);
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        if (connection == null) {
            connection = ConnectionFactory.createConnection(configuration);
        }
        LOG.info("HBase client build success!");
    }*/

    public List<JSONObject> getDataByRowKey(String tableName, String rowKey, String family) throws IOException {
        List<JSONObject> list = Lists.newArrayList();
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            // 如果列族不为空
            if (null != family && family.length() > 0) {
                get.addFamily(Bytes.toBytes(family));
            }
            Result result = table.get(get);
            List<Cell> cs = result.listCells();
            if (null == cs || cs.size() == 0) {
                return Lists.newArrayList();
            }
            JSONObject jsonObject = new JSONObject();
            for (Cell cell : cs) {
                jsonObject.put("rowKey", Bytes.toString(CellUtil.cloneRow(cell)));// 取行健
                jsonObject.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
            }
            list.add(jsonObject);
        }
        return list;
    }

    public void close() throws IOException {
        connection.close();
    }

    public Connection getConnection() {
        return connection;
    }
}
