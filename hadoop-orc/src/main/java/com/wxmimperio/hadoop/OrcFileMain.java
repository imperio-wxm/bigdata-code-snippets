package com.wxmimperio.hadoop;

import com.wxmimperio.hadoop.pojo.HiveColumnTypeDesc;
import com.wxmimperio.hadoop.utils.OrcUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public class OrcFileMain {

    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        String url = "jdbc:hive2://10.1.8.209:10000/dw";
        String username = "hadoop";
        try (Connection connection = DriverManager.getConnection(url, username, "")) {
            List<HiveColumnTypeDesc> hiveColumnTypeDescs = OrcUtils.getPartitionColumns("dw", "cd_item_glog", connection);
            System.out.println(hiveColumnTypeDescs);
        }
    }
}
