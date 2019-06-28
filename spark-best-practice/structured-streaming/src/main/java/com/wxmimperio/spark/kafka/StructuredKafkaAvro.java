package com.wxmimperio.spark.kafka;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wxmimperio.spark.common.HttpClientUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.Trigger;

import static org.apache.spark.sql.avro.package$.MODULE$;
import static org.apache.spark.sql.functions.col;

import java.util.concurrent.TimeUnit;

public class StructuredKafkaAvro {

    public static void main(String[] args) throws Exception {

        String res = HttpClientUtil.doGet("http://10.1.8.206:8081/subjects/test_pressure_1/versions/latest");

        JsonObject schema = new JsonParser().parse(res).getAsJsonObject();
        SparkConf conf = new SparkConf();
        conf.setAppName("StructuredKafkaAvro");
        conf.setMaster("local");

        // msg {"name":2,"event_time":"2019-06-20 19:09:48"}
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.1.8.132:9092")
                .option("subscribe", "test_pressure_1")
                .load();


        /*df.select(MODULE$.from_avro(col("value"), schema.get("schema").getAsString()).as("wxm_test_streaming"))
                .map((MapFunction<Row, String>) row -> {
                    GenericRowWithSchema rowWithSchema = (GenericRowWithSchema) row;
                    JsonObject jsonObject = new JsonObject();
                    for (int i = 0; i < rowWithSchema.size(); i++) {
                        GenericRowWithSchema realRow = (GenericRowWithSchema) rowWithSchema.get(i);
                        String[] colName = realRow.schema().fieldNames();
                        for (int j = 0; j < colName.length; j++) {
                            jsonObject.addProperty(colName[j], String.valueOf(realRow.get(j)));
                        }
                    }
                    return jsonObject.toString();
                }, Encoders.STRING())
                .writeStream()
                .format("console")
                .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
                .foreachBatch((VoidFunction2<Dataset<String>, Long>) (stringDataset, aLong) -> stringDataset.foreach((ForeachFunction<String>) System.out::println))
                .start()
                .awaitTermination();*/

        Dataset<Row> kafkaTable = df.select(MODULE$.from_avro(col("value"), schema.get("schema").getAsString()).as("wxm_test_streaming"));
        kafkaTable.createOrReplaceTempView("wxm_test_streaming");
        Dataset<Row> streamingSql = spark.sql("select wxm_test_streaming.event_time from wxm_test_streaming");
        streamingSql.writeStream()
                .outputMode(OutputMode.Append())
                .format("console")
                .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
                .start()
                .awaitTermination();
    }
}
