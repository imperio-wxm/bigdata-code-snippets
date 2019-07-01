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

    // kafka to kafka 只能保证  at least once 语义

    public static void main(String[] args) throws Exception {
        String checkPointPath = "D:\\d_backup\\github\\hadoop-code-snippets\\spark-best-practice\\structured-streaming\\checkpoint";

        SparkConf conf = new SparkConf();
        conf.setAppName("StructuredKafkaConnect");
        conf.setMaster("local");

        // msg {"name":2,"event_time":"2019-06-20 19:09:48"}
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.1.8.132:9092")
                .option("subscribe", "wxm_test_streaming")
                .load();

        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .map((MapFunction<Row, String>) row -> {
                    JsonObject jsonObject = new JsonParser().parse(row.getString(1)).getAsJsonObject();
                    return jsonObject.get("event_time").getAsString();
                }, Encoders.STRING())
                .writeStream()
                .trigger(Trigger.ProcessingTime(2, TimeUnit.SECONDS))
                //.format("console")
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.1.8.132:9092")
                .option("topic", "wxm_test")
                .option("checkpointLocation", checkPointPath)
                .start()
                .awaitTermination();
    }
}
