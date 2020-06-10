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

public class StructuredKafkaCheckPoint {

    /**
     * 建立单partition topic 测试checkpoint 是否可以断点消费
     * checkpoint 后，删除checkpoint offset丢失，从最新的开始消费
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        String resultPath = "D:\\d_backup\\github\\hadoop-code-snippets\\spark-best-practice\\structured-streaming\\result";
        String checkPointPath = "D:\\d_backup\\github\\hadoop-code-snippets\\spark-best-practice\\structured-streaming\\checkpoint";

        SparkConf conf = new SparkConf();
        conf.setAppName("StructuredKafkaCheckPoint");
        conf.setMaster("local");

        // msg {"name":2,"event_time":"2019-06-20 19:09:48"}
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.1.8.132:9092")
                .option("subscribe", "wxm_test_streaming")
                .load();

        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)", "CAST(partition AS STRING)", "CAST(offset AS STRING)", "CAST(timestamp AS STRING)", "CAST(timestampType AS STRING)")
                .map((MapFunction<Row, String>) row -> {
                    JsonObject jsonObject = new JsonObject();
                    String[] colName = row.schema().fieldNames();
                    for (int i = 0; i < colName.length; i++) {
                        jsonObject.addProperty(colName[i], (String) row.get(i));
                    }
                    return jsonObject.toString();
                }, Encoders.STRING())
                .writeStream()
                //.trigger(Trigger.ProcessingTime(2, TimeUnit.SECONDS))
                .format("text")
                .option("path", resultPath)
                .option("checkpointLocation", checkPointPath)
                .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
                .start()
                .awaitTermination();
    }

}
