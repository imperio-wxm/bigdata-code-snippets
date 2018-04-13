package com.wxmimperio.hbase;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import net.iharder.base64.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class HBaseSnapshotReader {
    private static final Log LOG = LogFactory.getLog(HBaseSnapshotReader.class);

    private static String HBASE_SITE = "hbaes-site.xml";

    public static String convertScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }

    public static class RowKeyMapper extends TableMapper<ImmutableBytesWritable, Result> {
        private static long vkey = 0L;
        private long mkey = 0L;

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            vkey = vkey + 1;
            mkey = vkey / 10;
            context.write(new ImmutableBytesWritable(String.valueOf(mkey).getBytes()), value);
            LOG.info("=======" + Bytes.toString(value.getRow()));
        }
    }

    public static class RowKeyReducer extends Reducer<ImmutableBytesWritable, Result, Text, Text> {
        @Override
        public void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
            for (Result result : values) {
                context.write(new Text(Bytes.toString(key.get())), new Text(Bytes.toString(result.getRow())));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        String tableName = args[0];
        String snapshotName = args[1];
        String tmpDir = args[2];
        String outputPath = args[3];

        Configuration conf = HBaseConfiguration.create();
        conf.addResource(HBASE_SITE);

        Scan scan = new Scan();
        scan.setBatch(5000);
        scan.setCacheBlocks(false);
        scan.setTimeRange(1523419740000L, 1523419800000L);

        Job job = new Job(conf, "HBase Export");
        job.setJarByClass(HBaseSnapshotReader.class);

        TableMapReduceUtil.initTableSnapshotMapperJob(
                snapshotName,
                scan,
                RowKeyMapper.class,
                NullWritable.class,
                Result.class,
                job,
                true,
                new Path(tmpDir)
        );
        job.setNumReduceTasks(50);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Result.class);
        job.setReducerClass(RowKeyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
}
