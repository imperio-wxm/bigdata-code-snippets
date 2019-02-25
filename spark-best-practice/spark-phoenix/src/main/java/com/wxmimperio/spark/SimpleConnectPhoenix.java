package com.wxmimperio.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

public class SimpleConnectPhoenix {

    public static void main(String[] args) {
        ResourceBundle rb = ResourceBundle.getBundle("application");
        String table = rb.getString("spark.phoenix.table");
        String zkUrl = rb.getString("spark.phoenix.zkUrl");
        String source = rb.getString("spark.phoenix.source");

        Map<String, String> props = new HashMap<>();
        props.put("table", table);
        props.put("zkUrl", zkUrl);
        
        SparkSession spark = SparkSession.builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();
        SQLContext sqlContext = new SQLContext(spark);
        Dataset<Row> row = sqlContext.read().format(source).options(props).load();
        row.createOrReplaceTempView("TEST_YWZX_WUHAN_SWITCH");

        Dataset<Row> dataset = spark.sql("select * from TEST_YWZX_WUHAN_SWITCH limit 10");
        dataset.show();

      /*  Dataset<Row> dataset1 = spark.sql("select count(*),SWITCH_OUTPUT_BROADCASTPKTS_DELTA,event_time as xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx from TEST_YWZX_WUHAN_SWITCH where event_time >= '2018-10-24 00:00:00' AND event_time <= '2018-10-24 01:00:00' group by SWITCH_OUTPUT_BROADCASTPKTS_DELTA,event_time limit 5000");
        dataset1.show();*/
    }
}
