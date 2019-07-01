package com.wxmimperio.spark.official.demo;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.concurrent.TimeUnit;

public class StructredKafkaJoinFile {

    public static void main(String[] args) throws Exception {

        // checkpoint 后聚合结果会被保存，程序挂掉会从checkpoint 点继续聚合
        String checkPointPath = "D:\\d_backup\\github\\hadoop-code-snippets\\spark-best-practice\\structured-streaming\\checkpoint";

        SparkConf conf = new SparkConf();
        conf.setAppName("StructredKafkaJoinFile");
        conf.setMaster("local");
        // 设置并行度为1，避免本地调试过多task
        conf.set("spark.default.parallelism", "1");
        conf.set("spark.sql.shuffle.partitions", "1");

        // msg {"name":2,"event_time":"2019-06-20 19:09:48"}
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.1.8.132:9092")
                .option("subscribe", "wxm_test_streaming")
                //.option("startingOffsets", "latest")
                .load();

        Dataset<Row> words = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .map((MapFunction<Row, Tuple2<String, String>>) row -> {
                    JsonObject jsonObject = new JsonParser().parse(row.getString(1)).getAsJsonObject();
                    String eventTime = jsonObject.get("event_time").getAsString();
                    String name = jsonObject.get("name").getAsString();
                    return new Tuple2<>(eventTime, name);
                }, Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
                .selectExpr("_1 as event_time", "_2 as name");
        // 去重
        //.dropDuplicates("event_time");

        words.createOrReplaceTempView("wxm_test");

        Dataset<Row> staticDf = spark.read().json("structured-streaming/src/main/resources/people.json");
        staticDf.createOrReplaceTempView("people");
        staticDf.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
        staticDf.show();

        // 聚合操作会触发 shuffle
        Dataset<Row> streamingSql = spark.sql("select count(*),b.age from wxm_test as a left join people b on a.name = b.name group by b.age");
        //streamingSql.checkpoint(true);
        // Dataset<Row> streamingSql = spark.sql("select * from wxm_test");
        streamingSql.writeStream()
                // Output mode
                .outputMode(OutputMode.Complete())
                // Details of the output sink
                .format("console")
                // text文件输出，option(path,"");
                //.format("text")
                // Trigger interval(Optionally)
                //.option("checkpointLocation", checkPointPath)
                .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
                .start()
                .awaitTermination();
    }
}
