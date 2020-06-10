package com.wxmimperio.spark.deltalake;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.avro.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.avro.package$.MODULE$;
import static org.apache.spark.sql.functions.col;

public class StructuredDeltaLakeKafkaSink {

    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        String res = "";
        JsonObject schema = new JsonParser().parse(res).getAsJsonObject();


        SparkConf conf = new SparkConf();
        conf.setAppName("StructuredDeltaLakeKafkaSink");
        //conf.setMaster("local");
        conf.set("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore");

        // msg {"name":2,"event_time":"2019-06-20 19:09:48"}
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        Dataset<Row> kafkaDf = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.1.8.132:9092")
                .option("subscribe", "test")
                .load();

        Dataset<Row> kafkaTable = kafkaDf.select(MODULE$.from_avro(col("value"), schema.get("schema").getAsString()).as("value"))
                .selectExpr(getSelectExprStr(schema));

        kafkaTable.writeStream()
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", "/wxm/checkpoint/test")
                .start("/wxm/delta/test")
                .awaitTermination();
    }

    private static String[] getSelectExprStr(JsonObject schema) {
        Schema schemaResult = new Schema.Parser().parse(schema.get("schema").getAsString());
        List<Schema.Field> fields = schemaResult.getFields();
        int i = 0;
        String[] selectExpr = new String[fields.size()];
        for (Schema.Field field : fields) {
            selectExpr[i++] = "CAST(value." + field.name() + " AS " + field.schema().getTypes().get(0).getName().toUpperCase() + ")";
        }
        return selectExpr;
    }
}
