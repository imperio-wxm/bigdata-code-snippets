package com.wxmimperio.hbase;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wxmimperio.hbase.hbaseadmin.HbaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class HbaseMain {
    private static Logger LOG = LoggerFactory.getLogger(HbaseMain.class);


    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "E:\\software\\hadoop-2.6.0-cdh5.11.1");

        HbaseAdmin hbaseAdmin = new HbaseAdmin();
        try {
            hbaseAdmin.createTable("test_table_1207", new String[]{"cf1", "cf2"});
            for (int i = 1; i <= 10; i++) {
                UUID uuid = UUID.randomUUID();
                String rowKey = String.valueOf(System.currentTimeMillis()) + "_" + uuid.toString().substring(0, 8);
                List<JsonObject> jsonObjects = new ArrayList<JsonObject>();
                jsonObjects.add(new JsonParser().parse("{\"name\":\"wxm" + i + "\",\"age\":25" + i + "}").getAsJsonObject());

                //hbaseAdmin.insterRow("test_table_1207", rowKey, "cf1", "area_id_" + i, "mid_" + i);
                //hbaseAdmin.insterRow("test_table_1207", "rw" + i + "_" + i, "cf2", "f" + i, "val_f" + i);
                hbaseAdmin.insertJsonRow("test_table_1207", rowKey, "cf1", jsonObjects);
            }
            //hbaseAdmin.scanData("test_table_1207", "1513076148015_", "1513076148131_");
            hbaseAdmin.close();
        } catch (Exception e) {
            LOG.error("error.", e);
        }
    }
}
