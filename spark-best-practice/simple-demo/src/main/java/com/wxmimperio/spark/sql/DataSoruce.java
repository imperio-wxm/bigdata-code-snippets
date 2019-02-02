package com.wxmimperio.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class DataSoruce {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();

        String jsonPath = "D:\\d_backup\\github\\hadoop-code-snippets\\spark-best-practice\\simple-demo\\src\\resources\\people.json";

        // format: json, parquet, jdbc, orc, libsvm, csv, text
        Dataset<Row> dataset = spark.read().format("json").load(jsonPath).as("people");
        dataset.select("age").show();
        // SaveMode.Append,SaveMode.ErrorIfExists,SaveMode.Overwrite,SaveMode.Ignore
        // dataset.write().mode(SaveMode.Append).format("json").save("result");

        // orc 格式必须有hive支持
        dataset.write()
                .mode(SaveMode.Append)
                .format("parquet")
                .option("path", "D:\\d_backup\\github\\hadoop-code-snippets\\spark-best-practice\\table")
                .saveAsTable("personTable");
    }
}
