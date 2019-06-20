package com.wxmimperio.spark.official.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;

public class JavaStructuredNetworkWordCount {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setAppName("JavaStructuredNetworkWordCount");
        conf.setMaster("local");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        Dataset<Row> lines = spark.readStream().format("socket").option("host", "127.0.0.1").option("port", 8999).load();
        Dataset<String> words = lines
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        Dataset<Row> wordCounts = words.groupBy("value").count();
        StreamingQuery query = wordCounts.writeStream().outputMode("complete").format("console").start();
        query.awaitTermination();
    }
}
