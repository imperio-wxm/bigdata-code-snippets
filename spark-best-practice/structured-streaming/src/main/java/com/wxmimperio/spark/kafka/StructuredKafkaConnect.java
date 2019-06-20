package com.wxmimperio.spark.kafka;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;


import java.util.concurrent.TimeUnit;

public class StructuredKafkaConnect {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setAppName("StructuredKafkaConnect");
        conf.setMaster("local");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "127.0.0.1:9092")
                .option("subscribe", "wxm_test")
                .load();


        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .map(new MapFunction<Row, String>() {
                    @Override
                    public String call(Row row) throws Exception {
                        JsonObject jsonObject = new JsonParser().parse(row.getString(1)).getAsJsonObject();
                        return jsonObject.get("event_time").getAsString();
                    }
                }, Encoders.STRING())
                .writeStream()
                .trigger(Trigger.ProcessingTime(2, TimeUnit.SECONDS))
                .format("console")
                .start().awaitTermination();

    }
}
