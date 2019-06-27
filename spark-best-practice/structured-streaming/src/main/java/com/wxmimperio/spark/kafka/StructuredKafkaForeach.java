package com.wxmimperio.spark.kafka;

import com.google.gson.JsonObject;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeUnit;

public class StructuredKafkaForeach {

    /*
     *  foreachBatch：
     *   1. 一个batch，微批次的数据处理
     *   2. 可以通过foreachBatch向多个ouptput 输出，但输出前需要对df进行缓存，因为每一次输出都会导致重新计算一次
     *   3. 默认情况，foreachBatch只保证 at-least-once 语义，可以通过batchId 在下游保证去重
     *   4. foreachBatch不支持持续计算，持续计算输出用foreach
     *
     *  foreach：
     *   1. 一个实例只负责一个分区的数据输出
     *   2. 实例必须可序列化
     *   3. 在 open 方法之后进行 连接初始化操作，保证输出连接正常
     *   4. 重写方法的生命周期：
     *      For each partition with partition_id：
     *          For each batch/epoch of streaming data with epoch_id：
     *              open(); // 如果 open方法返回true，则会调用process方法
     *              process();
     *              close(); // process中发生的任何error都会到这个方法；jvm crashes 可能不会调用这个方法
     *   5. partitionId 和 epochId 在micro-batch mode 时可用于下游数据去重
     */

    public static void main(String[] args) throws Exception {
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
                .writeStream()
                .format("console")
                .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
                /*.foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
                    @Override
                    public void call(Dataset<Row> rowDataset, Long batchId) throws Exception {
                        rowDataset.persist();
                        System.out.println("batchId = " + batchId);
                        rowDataset.show();
                    }
                })*/
                .foreach(new ForeachWriter<Row>() {
                    @Override
                    public boolean open(long partitionId, long epochId) {
                        System.out.println("=== partitionId = " + partitionId);
                        System.out.println("=== epochId = " + epochId);
                        return true;
                    }

                    @Override
                    public void process(Row row) {
                        JsonObject jsonObject = new JsonObject();
                        String[] colNames = row.schema().fieldNames();
                        for (int i = 0; i < colNames.length; i++) {
                            jsonObject.addProperty(colNames[i], (String) row.get(i));
                        }
                        System.out.println(jsonObject);
                    }

                    @Override
                    public void close(Throwable throwable) {
                        if (null != throwable) {
                            throwable.printStackTrace();
                        }
                    }
                })
                .start()
                .awaitTermination();

    }
}
