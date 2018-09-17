package com.wxmimperio.hbase;


import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class HFileGeneratorJson {
    private static Logger LOG = LoggerFactory.getLogger(HFileGeneratorJson.class);
    private static String KEY = "1111111111111111";

    public enum Count {
        TotalCount
    }

    public static class HFileMapper extends Mapper<Object, Text, ImmutableBytesWritable, Put> {
        private static final byte[] FAMILY_BYTE = Bytes.toBytes("c");
        private static SecretKey secretKey = initKey(KEY);
        private static Cipher cipher;

        static {
            try {
                cipher = Cipher.getInstance("AES");
                if (secretKey != null) {
                    cipher.init(Cipher.ENCRYPT_MODE, secretKey);
                } else {
                    LOG.info("key is null");
                    System.exit(1);
                }
            } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
                LOG.error("Get cipher error!!", e);
                System.exit(1);
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 通过metaRowKey=rowkey1|rowkey2...，自动生成组合rowkey
            String[] rowKeys = context.getConfiguration().get("metaRowKey").split("\\|", -1);
            JsonObject jsonObject = new JsonParser().parse(value.toString()).getAsJsonObject();
            String[] encodeKey = context.getConfiguration().get("encodeKey").split("\\|", -1);

            JsonObject data = new JsonObject();
            List<String> colList = new ArrayList<>();
            for (Map.Entry<String, JsonElement> jsonElementEntry : jsonObject.entrySet()) {
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
                    Base64.Encoder encoder = Base64.getEncoder().withoutPadding();
                    byte[] res = new byte[0];
                    try {
                        res = cipher.doFinal(encodeMsg, 0, encodeMsg.length);
                    } catch (IllegalBlockSizeException | BadPaddingException e) {
                        LOG.error("RowKey = " + StringUtils.join(rowKeys, "|") + " data = " + data + ", cipher error", e);
                    }
                    String encodedText = encoder.encodeToString(res);
                    LOG.info("=================");
                    LOG.info("加密前 = " + data);
                    data.addProperty(encodeCol, encodedText);
                    LOG.info("加密后 = " + data);
                }
            }

            StringBuilder rowKeyStr = new StringBuilder();
            for (String rowKey : rowKeys) {
                if (data.has(rowKey)) {
                    rowKeyStr.append(data.get(rowKey).getAsString()).append("|");
                }
            }
            if (rowKeyStr.length() > 0) {
                rowKeyStr.deleteCharAt(rowKeyStr.length() - 1);
                ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(StringUtils.isEmpty(rowKeyStr.toString()) ? "empty_rowkey" : rowKeyStr.toString()));
                for (Map.Entry<String, JsonElement> colSet : data.entrySet()) {
                    String colName = colSet.getKey();
                    String colValue = colSet.getValue().getAsString();
                    Put put = new Put(rowKey.copyBytes());
                    put.addColumn(FAMILY_BYTE, Bytes.toBytes(colName), Bytes.toBytes(StringUtils.isEmpty(colValue) ? "" : colValue));
                    context.write(rowKey, put);
                }
                context.getCounter(Count.TotalCount).increment(1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            LOG.error("Params size error! args = " + Arrays.asList(args));
            System.exit(1);
        }

        String tableName = args[0];
        String inputPath = args[1];
        String outputPath = args[2];
        String metaRowKey = args[3];
        String encodeKey = args[4];


        LOG.info("start config ================");
        LOG.info("tableName = " + tableName);
        LOG.info("inputPath = " + inputPath);
        LOG.info("outputPath = " + outputPath);
        LOG.info("metaRowKey = " + metaRowKey);
        LOG.info("encodeKey = " + encodeKey);
        LOG.info("end config ================");

        Configuration conf = HBaseConfiguration.create();
        conf.addResource("hbaes-site.xml");
        conf.set("metaRowKey", metaRowKey);
        conf.set("encodeKey", encodeKey);

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        String keyStruct = table.getTableDescriptor().getValue("key_struct");
        LOG.info("keyStruct = " + keyStruct);
        if (null != keyStruct && !metaRowKey.equals(keyStruct)) {
            conf.set("metaRowKey", keyStruct);
            LOG.warn("keyStruct = " + keyStruct + ", metaRowKey = " + metaRowKey + " please check!!!!");
        }

        Job job = Job.getInstance(conf, tableName + " hFile Generator");
        job.setJarByClass(HFileGeneratorJson.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(HFileMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setNumReduceTasks(500);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(tableName));

        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
        HFileOutputFormat2.setOutputPath(job, new Path(outputPath));

        if (job.waitForCompletion(true)) {
            FileSystem fs = FileSystem.get(conf);
            FsPermission changedPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
            fs.setPermission(new Path(outputPath), changedPermission);
            List<String> files = getAllFilePath(new Path(outputPath), fs);
            for (String file : files) {
                fs.setPermission(new Path(file), changedPermission);
            }

            // load file to hbase
            //LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            //loader.doBulkLoad(new Path(outputPath), (HTable) table);

            Counters counters = job.getCounters();
            Counter counter = counters.findCounter(Count.TotalCount);
            LOG.info("Hfile generator finished. TotalCount = " + counter.getValue() + ", Hfile path  = " + outputPath);
        }
    }

    private static SecretKey initKey(String key) {
        try {
            return new SecretKeySpec(key.getBytes(), 0, key.getBytes().length, "AES");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    private static List<String> getAllFilePath(Path filePath, FileSystem fs) throws IOException {
        List<String> fileList = new ArrayList<String>();
        FileStatus[] fileStatus = fs.listStatus(filePath);
        for (FileStatus fileStat : fileStatus) {
            if (fileStat.isDirectory()) {
                fileList.add(fileStat.getPath().toString());
                fileList.addAll(getAllFilePath(fileStat.getPath(), fs));
            } else {
                fileList.add(fileStat.getPath().toString());
            }
        }
        return fileList;
    }
}
