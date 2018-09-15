package com.wxmimperio.hbase;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.twitter.chill.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
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
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.util.*;

public class SparkSnapshotHFile {

    private static Logger LOG = LoggerFactory.getLogger(SparkSnapshotHFile.class);
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

            Broadcast<Map<String, String>> broadcast = sc.broadcast(map);

            JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(
                    job.getConfiguration(),
                    TableSnapshotInputFormat.class,
                    ImmutableBytesWritable.class,
                    Result.class
            );

            JavaRDD<String> hbaseLineRDD = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
                @Override
                public String call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
                    JsonObject jsonObject = convertResultToJson(v1._2);
                    return jsonObject.toString();
                }
            });

            JavaRDD<String> javaRDD = hbaseLineRDD.persist(StorageLevel.MEMORY_AND_DISK());

            javaRDD.flatMapToPair(new PairFlatMapFunction<String, ImmutableBytesWritable, KeyValue>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Iterator<Tuple2<ImmutableBytesWritable, KeyValue>> call(String text) throws Exception {
                    List<Tuple2<ImmutableBytesWritable, KeyValue>> tps = new ArrayList<>();

                    if (null == text || text.length() < 1 || text.equalsIgnoreCase(new JsonParser().toString())) {
                        //不能返回null
                        return tps.iterator();
                    }
                    Map<String, String> broadcastValue = broadcast.getValue();
                    JsonObject tableDetail = new JsonParser().parse(text).getAsJsonObject();
                    String[] rowKeys = broadcastValue.get("metaRowKey").split("\\|", -1);
                    String[] encodeKey = broadcastValue.get("encodeKey").split("\\|", -1);

                    String key = "1111111111111111";
                    SecretKey secretKey = new SecretKeySpec(key.getBytes(), 0, key.getBytes().length, "AES");
                    Cipher cipher = Cipher.getInstance("AES");
                    cipher.init(Cipher.ENCRYPT_MODE, secretKey);

                    JsonObject data = new JsonObject();
                    List<String> colList = new ArrayList<>();
                    for (Map.Entry<String, JsonElement> jsonElementEntry : tableDetail.entrySet()) {
                        String colKey = jsonElementEntry.getKey();
                        String colValue = jsonElementEntry.getValue().getAsString();
                        if (!StringUtils.isEmpty(colValue) && !colValue.equalsIgnoreCase("null") && !colValue.equalsIgnoreCase("\\N")) {
                            data.addProperty(colKey, colValue);
                            colList.add(colKey);
                        }
                    }

                    for (String encodeCol : encodeKey) {
                        if (data.has(encodeCol) && null != data.get(encodeCol) && !StringUtils.isEmpty(data.get(encodeCol).getAsString())) {
                            // 加密
                            byte[] encodeMsg = data.get(encodeCol).getAsString().getBytes();
                            java.util.Base64.Encoder encoder = java.util.Base64.getEncoder().withoutPadding();
                            byte[] res = new byte[0];
                            try {
                                res = cipher.doFinal(encodeMsg, 0, encodeMsg.length);
                            } catch (IllegalBlockSizeException | BadPaddingException e) {
                                LOG.error("RowKey = " + StringUtils.join(rowKeys, "|") + " data = " + data + ", cipher error", e);
                            }
                            String encodedText = encoder.encodeToString(res);
                            data.addProperty(encodeCol, encodedText);
                        }
                    }

                    // 列的顺序要按字典序
                    Collections.sort(colList);
                    JsonObject newData = new JsonObject();
                    for (String colName : colList) {
                        newData.addProperty(colName, data.get(colName).getAsString());
                    }
                    data = newData;

                    StringBuilder rowKeyStr = new StringBuilder();
                    for (String rowKey : rowKeys) {
                        if (data.has(rowKey)) {
                            rowKeyStr.append(data.get(rowKey).getAsString()).append("|");
                        }
                    }

                    if (rowKeyStr.length() > 0) {
                        rowKeyStr.deleteCharAt(rowKeyStr.length() - 1);
                        String finalRowKey = StringUtils.isEmpty(rowKeyStr.toString()) ? "empty_rowkey" : rowKeyStr.toString();
                        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(finalRowKey));

                        for (Map.Entry<String, JsonElement> colSet : data.entrySet()) {
                            String colName = colSet.getKey();
                            String colValue = colSet.getValue().getAsString();
                            tps.add(new Tuple2<>(rowKey, new KeyValue(Bytes.toBytes(finalRowKey), FAMILY_BYTE, Bytes.toBytes(colName), Bytes.toBytes(StringUtils.isEmpty(colValue) ? "" : colValue))));
                        }
                    }
                    return tps.iterator();
                }
            }).sortByKey().saveAsNewAPIHadoopFile(outputPath, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, conf);

            LOG.info("all size = " + javaRDD.count());
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
