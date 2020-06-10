package com.wxmimperio.spark;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

public class SimpleConnectPhoenix {

    /**
     * /app/opt/cloudera/parcels/SPARK2-2.2.0.cloudera2-1.cdh5.12.0.p0.232957/bin/spark2-submit --class com.wxmimperio.spark.SimpleConnectPhoenix --master yarn --deploy-mode cluster --executor-memory 6G --num-executors 16 --jars hdfs://sdg/user/hadoop/phoneix_support/phoenix-4.13.0-cdh5.11.1-sdg-1.0.0-RELEASE-client.jar /home/hadoop/wxm/spark/spark-phoenix-1.0-SNAPSHOT.jar
     * @param args
     */

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
                //.master("local")
                .getOrCreate();
        SQLContext sqlContext = new SQLContext(spark);
        Dataset<Row> row = sqlContext.read().format(source).options(props).load();
        row.createOrReplaceTempView("TEST_YWZX_WUHAN_SWITCH");

        Dataset<Row> dataSet = spark.sql("select count(*),switch_host from TEST_YWZX_WUHAN_SWITCH where event_time >= '2018-10-23 00:00:00' and event_time <= '2018-11-16 00:00:00' group by switch_host");
        dataSet.show();

      /*  Dataset<Row> dataset1 = spark.sql("select count(*),SWITCH_OUTPUT_BROADCASTPKTS_DELTA,event_time as xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx from TEST_YWZX_WUHAN_SWITCH where event_time >= '2018-10-24 00:00:00' AND event_time <= '2018-10-24 01:00:00' group by SWITCH_OUTPUT_BROADCASTPKTS_DELTA,event_time limit 5000");
        dataset1.show();*/
    }
}
