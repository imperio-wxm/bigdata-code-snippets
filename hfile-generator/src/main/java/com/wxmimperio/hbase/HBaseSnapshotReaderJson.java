package com.wxmimperio.hbase;

import com.google.gson.JsonObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public class HBaseSnapshotReaderJson {
    private static Logger LOG = LoggerFactory.getLogger(HBaseSnapshotReaderJson.class);


    private static String EMPTY_JSON = new JsonObject().toString();

    public enum Count {
        TotalCount
    }

    public static class SnapshotMapper extends TableMapper<ImmutableBytesWritable, Result> {
        private static long vkey = 0L;
        private long mkey = 0L;

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            vkey = vkey + 1;
            mkey = vkey / 10000;
            context.write(new ImmutableBytesWritable(Bytes.toBytes(mkey)), value);
        }
    }

    public static class SnapshotReducer extends Reducer<ImmutableBytesWritable, Result, NullWritable, Text> {


        @Override
        public void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
            for (Result result : values) {
                JsonObject jsonData = convertResultToJson(result);
                if (jsonData.toString().equalsIgnoreCase(EMPTY_JSON)) {
                    continue;
                }
                context.write(NullWritable.get(), new Text(jsonData.toString()));
                context.getCounter(Count.TotalCount).increment(1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String snapshotName = args[0];
        String snapshotPath = args[1];
        String root = args[2];
        String outputPath = args[3];


        Configuration conf = HBaseConfiguration.create();
        String hbaseSite = "hbaes-site.xml";
        conf.addResource(hbaseSite);
        // 设置hbase 根路径，以自动读取snapshot文件
        conf.set("hbase.rootdir", root);

        Scan scan = new Scan();
        scan.setBatch(5000);
        scan.setCacheBlocks(false);
        scan.setMaxVersions(1);

        Job job = new Job(conf, "Table = " + snapshotName + " snapshot reader!");
        job.setJarByClass(HBaseSnapshotReaderJson.class);
        TableMapReduceUtil.initTableSnapshotMapperJob(
                snapshotName,
                scan,
                SnapshotMapper.class,
                NullWritable.class,
                Result.class,
                job,
                true,
                new Path(snapshotPath)
        );
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Result.class);
        job.setReducerClass(SnapshotReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Writable.class);
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        job.setNumReduceTasks(200);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("Error with job!");
        }

        Counters counters = job.getCounters();
        Counter counter = counters.findCounter(Count.TotalCount);
        LOG.info("TotalCount = " + counter.getValue());
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
    }}
