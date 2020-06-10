package com.wxmimperio.spark.official.demo;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;

import java.util.Arrays;
import java.util.concurrent.*;

public class StructredStreamingManager {

    /*
     * 1. 一个 SparkSession 可以同时运行多个 streaming query，共享群集资源
     */

    public static void main(String[] args) throws Exception {

        // 单开一个定时线程 采集 metrics
        ScheduledExecutorService pool = Executors.newScheduledThreadPool(5);

        SparkConf conf = new SparkConf();
        conf.setAppName("StructredStreamingManager");
        conf.setMaster("local");

        // msg {"name":2,"event_time":"2019-06-20 19:09:48"}
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.1.8.132:9092")
                .option("subscribe", "wxm_test_streaming")
                .load();

        StreamingQuery query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .map((MapFunction<Row, String>) row -> {
                    JsonObject jsonObject = new JsonParser().parse(row.getString(1)).getAsJsonObject();
                    return jsonObject.get("event_time").getAsString();
                }, Encoders.STRING())
                .writeStream()
                .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
                .format("console")
                .queryName("test-xxx")
                .start();

        pool.scheduleAtFixedRate(() -> {
                    // get the unique identifier of the running query that persists across restarts from checkpoint data
                    // 获取从检查点数据重新启动后持续存在的运行查询的唯一标识符
                    System.out.println("====== query id = " + query.id());

                    // get the unique id of this run of the query, which will be generated at every start/restart
                    // 获取此运行查询的唯一ID，该ID将在每次启动/重新启动时生成
                    System.out.println("====== query runId = " + query.runId());

                    // get the name of the auto-generated or user-specified name
                    // 获取自动生成或用户指定名称的名称
                    System.out.println("====== query name = " + query.name());

                    // print detailed explanations of the query
                    // 打印查询的详细说明
                    query.explain();

                    // stop the query
                    // query.stop();

                    // the exception if the query has been terminated with error
                    // 如果查询已因错误而终止，则为异常
                    // query.exception();

                    // an array of the most recent progress updates for this query
                    // 此查询的最新进度更新数组
                    StreamingQueryProgress[] queryProgresses = query.recentProgress();
                    Arrays.asList(queryProgresses).forEach(processes -> {
                        System.out.println("====== processedRowsPerSecond =" + processes);
                    });

                    // the most recent progress update of this streaming query
                    // 此流式查询的最新进度更新
                    // 包含有关在流的最后一次触发中所取得进展的所有信息- 处理了哪些数据，处理速率，延迟等
                    System.out.println("====lastQueryProgress = " + query.lastProgress());

                    // 获取当前活动的流式查询列表
                    StreamingQuery[] streamingQueries = spark.streams().active();
                    Arrays.asList(streamingQueries).forEach(streamingQuery -> {
                        System.out.println(" ====== streamingQueries = " + streamingQuery);
                    });

                    // 提供了有关查询立即执行操作的信息 - 触发器是否处于活动状态，是否正在处理数据
                    System.out.println(query.status());
                },
                5,
                5,
                TimeUnit.SECONDS
        );

        // 异步监听 streaming 状态
        spark.streams().addListener(new StreamingQueryListener() {
            @Override
            public void onQueryStarted(QueryStartedEvent queryStartedEvent) {
                System.out.println("======  onQueryStarted =" + queryStartedEvent.id());
            }

            @Override
            public void onQueryProgress(QueryProgressEvent queryProgressEvent) {
                System.out.println("======  QueryProgressEvent =" + queryProgressEvent.progress());
            }

            @Override
            public void onQueryTerminated(QueryTerminatedEvent queryTerminatedEvent) {
                System.out.println("======  QueryTerminatedEvent =" + queryTerminatedEvent.id());
            }
        });

        query.awaitTermination();
    }
}
