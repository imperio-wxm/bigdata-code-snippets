package com.wxmimperio.spark.hive;

import org.apache.spark.sql.SparkSession;

import java.io.File;

public class StructuredHiveConnect {

    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();
        spark.sql("select * from dw.xxx where xxx='2019-06-03' limit 10").show();
        spark.stop();
    }
}
