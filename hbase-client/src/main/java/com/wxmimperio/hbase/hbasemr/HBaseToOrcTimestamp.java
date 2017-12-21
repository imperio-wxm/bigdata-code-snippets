package com.wxmimperio.hbase.hbasemr;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wxmimperio.hbase.HTableInputFormat;
import com.wxmimperio.hbase.pojo.HDFSFile;
import com.wxmimperio.hbase.utils.HDFSUtil;
import com.wxmimperio.hbase.utils.HiveUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


public class HBaseToOrcTimestamp {
    private static Logger LOG = LoggerFactory.getLogger(HBaseToOrcTimestamp.class);

    private static String HBASE_SITE = "hbaes-site.xml";
    public static String EMPTY = new String("");
    private static String TYPE_TIMESTAMP = "timestamp";
    private static String TYPE_ROWKEY = "rowkey";
    private static String TYPE_ROWKEY_TIMESTAMP = "timestamp";
    private final static List<String> scanType = new ArrayList<String>(Arrays.asList(TYPE_TIMESTAMP, TYPE_ROWKEY, TYPE_ROWKEY_TIMESTAMP));

    public static class HBaseMapper extends TableMapper<ImmutableBytesWritable, Text> {
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            context.write(new ImmutableBytesWritable("key".getBytes()), new Text(HiveUtil.getJsonCell(value).toString()));
        }
    }

    public static class HBaseReduce extends Reducer<ImmutableBytesWritable, Text, NullWritable, Writable> {
        private final OrcSerde orcSerde = new OrcSerde();
        private Writable row;

        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String schemaStr = context.getConfiguration().get("schema");
            TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(schemaStr);
            SettableStructObjectInspector inspector = (SettableStructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
            List<StructField> fields = (List<StructField>) inspector.getAllStructFieldRefs();
            OrcStruct orcStruct = (OrcStruct) inspector.create();
            orcStruct.setNumFields(fields.size());

            for (Text text : values) {
                JsonObject jsonData = new JsonParser().parse(text.toString()).getAsJsonObject();
                for (StructField structField : fields) {
                    if (jsonData.has(structField.getFieldName())) {
                        HiveUtil.formatFieldValue(inspector, structField, orcStruct, jsonData.get(structField.getFieldName()).getAsString());
                    } else {
                        HiveUtil.formatFieldValue(inspector, structField, orcStruct, EMPTY);
                    }
                }
                this.row = orcSerde.serialize(orcStruct, inspector);
                context.write(NullWritable.get(), this.row);
            }
        }
    }

    private static void runHBaseToOrc(String[] args) throws Exception {
        if (!scanType.contains(args[0])) {
            LOG.info("Scan type mast be in " + scanType);
            System.exit(2);
        }

        String scanType = args[0];
        String tableName = args[1];
        String partDate = args[2];
        String startRowKey = args[3];
        String endRowKey = args[4];
        String endTimestamp = args[3];
        String step = args[4];

        Configuration config = HBaseConfiguration.create();
        StructTypeInfo schema = HiveUtil.getColumnTypeDescs("dw", tableName);
        config.addResource(HBASE_SITE);
        config.set("schema", schema.getTypeName());
        config.set("orc.compress", "SNAPPY");
        config.set("mapreduce.output.basename", "orc");

        HDFSFile hdfsFile = new HDFSFile(tableName, partDate, config.get("hive.db.location"), endTimestamp, step);
        LOG.info(hdfsFile.toString());

        // add partition
        HiveUtil.addPartition("dw", tableName, partDate);
        // delete exists file
        List<String> files = HDFSUtil.getFileList(hdfsFile.getTempPath().replaceAll("/orc_temp", ""));
        List<String> deleteFilesName = HiveUtil.getDeleteFileName(hdfsFile.getTableName(), hdfsFile.getStartTimestamp(), hdfsFile.getEndTimestamp());
        LOG.info("Exist files = " + files.toString());
        LOG.info("Should be deleted files name = " + deleteFilesName.toString());
        for (String file : files) {
            for (String deleteName : deleteFilesName) {
                if (file.contains(deleteName) && HDFSUtil.isFileClosed(file)) {
                    HDFSUtil.deleteFile(file);
                }
            }
        }

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        if (scanType.equalsIgnoreCase(TYPE_TIMESTAMP)) {
            scan.setTimeRange(
                    Long.parseLong(hdfsFile.getStartTimestamp()),
                    Long.parseLong(hdfsFile.getEndTimestamp())
            );
        } else if (scanType.equalsIgnoreCase(TYPE_ROWKEY)) {
            scan.setStartRow(Bytes.toBytes(startRowKey));
            scan.setStopRow(Bytes.toBytes(endRowKey));
        }
        scan.setMaxVersions(1);

        Job job = new Job(config, "HBaseToOrc=" + hdfsFile.getFileName());
        job.setJarByClass(HBaseToOrcTimestamp.class);
        TableMapReduceUtil.initTableMapperJob(
                tableName,
                scan,
                HBaseMapper.class,
                null,
                null,
                job);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(HBaseReduce.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Writable.class);
        OrcNewOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        job.setOutputFormatClass(OrcNewOutputFormat.class);
        job.setNumReduceTasks(1);

        if (scanType.equalsIgnoreCase(TYPE_ROWKEY_TIMESTAMP)) {
            config.set("logical.scan.start", hdfsFile.getStartTimestamp());
            config.set("logical.scan.stop", hdfsFile.getEndTimestamp());
            config.set("start.null.slat", "0");
            config.set("end.null.slat", "10");
            job.setInputFormatClass(HTableInputFormat.class);
        }

        FileOutputFormat.setOutputPath(job, new Path(hdfsFile.getTempPath()));

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("Error with job!");
        }

        // move tempPath to realPath
        if (HDFSUtil.isFileClosed(hdfsFile.getMvPath())) {
            HDFSUtil.renameFile(hdfsFile.getMvPath(), hdfsFile.getRealPath());
        }
        // clear tempPath
        HDFSUtil.deleteFile(hdfsFile.getTempPath());
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            LOG.info("Params length error!" + Arrays.asList(args));
            LOG.info("Use: <scanType:rowKey> <tableName> <parDate> <startRowKey> <endRowKey>");
            LOG.info("Use: <scanType:rowKey_timestamp or timestamp> <tableName> <parDate> <endTimestamp> <step>");
            System.exit(2);
        }
        LOG.info("Params = " + Arrays.asList(args));
        runHBaseToOrc(args);
    }
}
