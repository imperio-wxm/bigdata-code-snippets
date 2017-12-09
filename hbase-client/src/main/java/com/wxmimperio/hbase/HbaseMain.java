package com.wxmimperio.hbase;

import com.wxmimperio.hbase.hbaseadmin.HbaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseMain {
    private static Logger LOG = LoggerFactory.getLogger(HbaseMain.class);


    public static void main(String[] args) {
        HbaseAdmin hbaseAdmin = new HbaseAdmin();

        try {
            hbaseAdmin.createTable("test_table_1207", new String[]{"cf1", "cf2"});
            for (int i = 1; i <= 10; i++) {
                hbaseAdmin.insterRow("test_table_1207", "rw" + i, "cf1", "q" + i, "val_q" + i);
                //hbaseAdmin.insterRow("test_table_1207", "rw" + i + "_" + i, "cf2", "f" + i, "val_f" + i);
                hbaseAdmin.scanData("test_table_1207", "rw1", "rw10");
                hbaseAdmin.getData("test_table_1207", "rw1", "cf1", "q1");
            }
            hbaseAdmin.close();
        } catch (Exception e) {
            LOG.error("error.", e);
        }
    }
}
