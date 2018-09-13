package com.wxmimperio.hbase;

import com.google.gson.JsonObject;
import com.twitter.chill.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class SparkSnapshotJson {

    private static Logger LOG = LoggerFactory.getLogger(SparkSnapshotJson.class);
    private static final byte[] FAMILY_BYTE = Bytes.toBytes("c");

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("HBase SnapshotReader");

        String snapName = args[0];
        String snapPath = args[1];
        String root = args[2];
        String metaRowKey = args[3];
        String encodeKeys = args[4];
        String outputPath = args[5];

        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            Configuration conf = HBaseConfiguration.create();
            conf.addResource("hbaes-site.xml");
            conf.set("hbase.rootdir", root);
            conf.set(TableInputFormat.SCAN, convertScanToString());
            Job job = Job.getInstance(conf, snapName + " hFile Generator");
            TableSnapshotInputFormat.setInput(job, snapName, new Path(snapPath));


            Map<String, String> map = new HashMap<>(2);
            map.put("metaRowKey", metaRowKey);
            map.put("encodeKey", encodeKeys);

            JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(
                    job.getConfiguration(),
                    TableSnapshotInputFormat.class,
                    ImmutableBytesWritable.class,
                    Result.class
            ).repartition(1000);

            JavaRDD<String> hbaseLineRDD = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
                @Override
                public String call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
                    return convertResultToJson(v1._2).toString();
                }
            });
            hbaseLineRDD.persist(StorageLevel.MEMORY_AND_DISK());
            hbaseLineRDD.saveAsTextFile(outputPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String convertScanToString() throws IOException {
        Scan scan = new Scan();
        scan.setBatch(5000);
        scan.setMaxVersions(1);
        scan.setCacheBlocks(false);
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }

    private static JsonObject convertResultToJson(Result value) {
        JsonObject jsonObject = new JsonObject();
        for (Cell cell : value.rawCells()) {
            String cellValue = new String(CellUtil.cloneValue(cell));
            if (!StringUtils.isEmpty(cellValue) && !cellValue.equalsIgnoreCase("null") && !cellValue.equalsIgnoreCase("\\N")) {
                jsonObject.addProperty(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
            }
        }
        return jsonObject;
    }
}
