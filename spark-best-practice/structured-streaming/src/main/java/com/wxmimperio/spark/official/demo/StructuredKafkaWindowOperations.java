package com.wxmimperio.spark.official.demo;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import scala.Tuple2;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class StructuredKafkaWindowOperations {

    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setAppName("StructuredKafkaWindowOperations");
        conf.setMaster("local");

        // msg {"name":2,"event_time":"2019-06-20 19:09:48"}
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.1.8.132:9092")
                .option("subscribe", "wxm_test")
                .load();

        Dataset<Tuple2<Timestamp, String>> words = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .map((MapFunction<Row, Tuple2<Timestamp, String>>) row -> {
                    JsonObject jsonObject = new JsonParser().parse(row.getString(1)).getAsJsonObject();
                    String eventTime = jsonObject.get("event_time").getAsString();
                    String name = jsonObject.get("name").getAsString();
                    Timestamp timestamp = new Timestamp(simpleDateFormat.parse(eventTime).getTime());
                    return new Tuple2<>(timestamp, name);
                }, Encoders.tuple(Encoders.TIMESTAMP(), Encoders.STRING()));

        // 滑动窗口，计算 2 min 的数据，每1 min 滑动一次
        words.groupBy(
                functions.window(words.col("_1"), "2 minutes", "1 minutes"),
                words.col("_2")
        )
                .count()
                .writeStream()
                .outputMode(OutputMode.Update())
                //.trigger(Trigger.ProcessingTime(2, TimeUnit.SECONDS))
                .format("console")
                .start()
                .awaitTermination();
    }
}
