package com.wxmimperio.spark.deltalake;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StructuredBatchDeltaLake {

    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();

        SparkConf conf = new SparkConf();
        conf.setAppName("StructureKafkaWindowSql");
        conf.set("spark.sql.warehouse.dir", warehouseLocation);
        conf.set("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore");
        //conf.setMaster("local");

        SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        Dataset<Row> kafkaDf = spark.read().format("delta").load("/wxm/delta/test");
        kafkaDf.createOrReplaceTempView("test");

        Dataset<Row> hiveDf = spark.sql("select * from dw_dev.test where part_date='2019-06-29'");
        hiveDf.createOrReplaceTempView("test");
        hiveDf.persist(StorageLevel.MEMORY_ONLY());

        spark.sql("OPTIMIZE delta.`/wxm/delta/test`");

        while (true) {
            spark.sql("select * from test").show();
            System.out.println(simpleDateFormat.format(new Date()));

            Thread.sleep(10000);
        }
    }
}
