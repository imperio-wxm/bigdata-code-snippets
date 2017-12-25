package com.wxmimperio.hbase;

import com.google.common.collect.Lists;
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

        List<JsonObject> jsonList = new ArrayList<JsonObject>();

        HbaseAdmin hbaseAdmin = new HbaseAdmin();
        try {
            hbaseAdmin.createTable("test_table_1849", new String[]{"cf1", "cf2"}, splitKeys);
            for (int i = 1; i <= 10; i++) {
                //hbaseAdmin.insterRow("test_table_1207", rowKey, "cf1", "area_id_" + i, "mid_" + i);
                //hbaseAdmin.insterRow("test_table_1207", "rw" + i + "_" + i, "cf2", "f" + i, "val_f" + i);
                //hbaseAdmin.insertJsonRow("test_table_1214", rowKey, "cf1", jsonObjects);
                //jsonList.add(new JsonParser().parse("{\"name\":\"wxm" + i + "\",\"age\":25" + i + "}").getAsJsonObject());

                jsonList.add(new JsonParser().parse("{\"name\":\"wxm" + i + "\"}").getAsJsonObject());

            }
            //hbaseAdmin.insertJsonRow("orc_rowkey_test", "cf1", jsonList);
            //hbaseAdmin.batchAsyncPut("test_table_1214", "cf1", jsonList);
            //hbaseAdmin.batchAsyncPut("test_table_1214", "cf1", jsonList);
            //hbaseAdmin.batchAsyncPut("test_table_1214", "cf1", jsonList);
            //hbaseAdmin.scanData("test_table_1207", "1513131122697_", "1513131122826_");

            System.out.println(hbaseAdmin.getData("orc_wooolh_money_consume_glog", "0|00000fd82570", null, null));
            System.out.println(hbaseAdmin.getByKeyList("orc_wooolh_money_consume_glog",
                    Lists.newArrayList("0|00000fd82570", "0|000003778278"), Lists.newArrayList("event_time"), null, null));
            hbaseAdmin.close();
        } catch (Exception e) {
            LOG.error("error.", e);
        }
    }
}
