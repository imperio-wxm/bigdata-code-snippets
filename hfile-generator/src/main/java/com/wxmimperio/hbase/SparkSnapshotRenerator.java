package com.wxmimperio.hbase;

import com.google.gson.JsonObject;
import com.twitter.chill.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.Iterator;


public class SparkSnapshotRenerator {

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("HBase SnapshotReader");

        String snapName = args[0];
        String snapPath = args[1];
        String root = args[2];

        Configuration conf = HBaseConfiguration.create();
        conf.addResource("hbaes-site.xml");
        conf.set("hbase.rootdir", root);
        conf.set(TableInputFormat.SCAN, convertScanToString());

        Job job = Job.getInstance(conf, snapName + " hFile Generator");

        TableSnapshotInputFormat.setInput(job, snapName, new Path(snapPath));

        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(
                    job.getConfiguration(),
                    TableSnapshotInputFormat.class,
                    ImmutableBytesWritable.class,
                    Result.class
            );

            JavaPairRDD<ImmutableBytesWritable, String> hbaseLineRDD = hbaseRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, ImmutableBytesWritable, String>() {
                @Override
                public Tuple2<ImmutableBytesWritable, String> call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
                    JsonObject jsonObject = convertResultToJson(v1._2);
                    return new Tuple2<>(v1._1, jsonObject.toString());
                }
            }).reduceByKey(new Function2<String, String, String>() {
                @Override
                public String call(String v1, String v2) throws Exception {
                    return v2;
                }
            });

            hbaseLineRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<ImmutableBytesWritable,String>, ImmutableBytesWritable, KeyValue>() {
                @Override
                public Iterator<Tuple2<ImmutableBytesWritable, KeyValue>> call(Tuple2<ImmutableBytesWritable, String> tuple2) throws Exception {
                    return null;
                }
            });

            hbaseLineRDD.take(10).forEach(line -> {
                System.out.println(line._2);
            });
        }
    }

    private static String convertScanToString() throws IOException {
        Scan scan = new Scan();
        scan.setMaxVersions(1);
        scan.setBatch(5000);
        scan.setCacheBlocks(false);
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }

    private static JsonObject convertResultToJson(Result value) {
        JsonObject jsonObject = new JsonObject();
        for (Cell cell : value.rawCells()) {
            jsonObject.addProperty("Rowkey", new String(CellUtil.cloneRow(cell)));
            jsonObject.addProperty(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
            cell.getTimestamp();
        }
        return jsonObject;
    }
}
