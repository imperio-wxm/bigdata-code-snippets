package com.wxmimperio.hbase;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wxmimperio.hbase.hbaseadmin.HbaseAdmin;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HbaseMain {
    private static Logger LOG = LoggerFactory.getLogger(HbaseMain.class);


    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "E:\\software\\hadoop-2.6.0-cdh5.11.1");

        byte[][] splitKeys = new byte[][]{
                Bytes.toBytes("1"), Bytes.toBytes("2"),
                Bytes.toBytes("3"), Bytes.toBytes("4"),
                Bytes.toBytes("5"), Bytes.toBytes("6"),
                Bytes.toBytes("7"), Bytes.toBytes("8"),
                Bytes.toBytes("9")};

        List<Map<String, JsonObject>> jsonList = new ArrayList<Map<String, JsonObject>>();

        HbaseAdmin hbaseAdmin = new HbaseAdmin();
        try {
            hbaseAdmin.createTable("test_table_1214", new String[]{"cf1", "cf2"}, splitKeys);
            for (int i = 1; i <= 10; i++) {
                UUID uuid = UUID.randomUUID();
                String logicalKey = String.valueOf(System.currentTimeMillis()) + "|" + uuid.toString().substring(0, 8);
                //String rowKey = StringUtils.leftPad(Integer.toString(Math.abs(logicalKey.hashCode() % 1000)), 3, "0") + "|" + logicalKey;
                String rowKey = Integer.toString(Math.abs(logicalKey.hashCode() % 1000)).substring(0, 1) + "|" + logicalKey;

                List<JsonObject> jsonObjects = new ArrayList<JsonObject>();
                jsonObjects.add(new JsonParser().parse("{\"name\":\"wxm" + i + "\",\"age\":25" + i + "}").getAsJsonObject());

                //hbaseAdmin.insterRow("test_table_1207", rowKey, "cf1", "area_id_" + i, "mid_" + i);
                //hbaseAdmin.insterRow("test_table_1207", "rw" + i + "_" + i, "cf2", "f" + i, "val_f" + i);
                //hbaseAdmin.insertJsonRow("test_table_1214", rowKey, "cf1", jsonObjects);

                Map<String, JsonObject> jsonMap = new HashMap<String, JsonObject>();
                jsonMap.put(rowKey, new JsonParser().parse("{\"name\":\"wxm" + i + "\",\"age\":25" + i + "}").getAsJsonObject());
                jsonList.add(jsonMap);
            }
            hbaseAdmin.insertJsonRow("test_table_1214", "cf1", jsonList);
            hbaseAdmin.insertJsonRow("test_table_1214", "cf1", jsonList);
            hbaseAdmin.insertJsonRow("test_table_1214", "cf1", jsonList);
            //hbaseAdmin.batchAsyncPut("test_table_1214", "cf1", jsonList);
            //hbaseAdmin.batchAsyncPut("test_table_1214", "cf1", jsonList);
            //hbaseAdmin.batchAsyncPut("test_table_1214", "cf1", jsonList);
            //hbaseAdmin.scanData("test_table_1207", "1513131122697_", "1513131122826_");
            hbaseAdmin.close();
        } catch (Exception e) {
            LOG.error("error.", e);
        }
    }
}
