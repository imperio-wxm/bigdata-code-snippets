package com.wxmimperio.hbase;

import com.google.gson.JsonObject;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class HbaseMapReduce {

    public static class HBaseMapper extends TableMapper<ImmutableBytesWritable, Text> {
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            // process data for the row from the Result instance.
            context.write(new ImmutableBytesWritable("key".getBytes()), new Text(getJsonCell(value).toString()));
        }
    }

    public static class HbaseReduce extends Reducer<ImmutableBytesWritable, Text, Text, Text> {

        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text text : values) {
                context.write(new Text(), text);
            }
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String tableName = "test_table_1207";
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        Job job = new Job(config, "HBaseMapReduceRead");
        job.setJarByClass(HbaseMapReduce.class);

        String startRow = "1513131122697_";
        String stopRow = "1513131122845_";
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));

        TableMapReduceUtil.initTableMapperJob(
                tableName,
                scan,
                HBaseMapper.class,
                null,
                null,
                job);
        //job.setOutputFormatClass(NullOutputFormat.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(HbaseReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path("/wxm/hbase_test/result.txt"));

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }

    public static JsonObject getJsonCell(Result value) {
        JsonObject jsonObject = new JsonObject();
        for (Cell cell : value.rawCells()) {
            jsonObject.addProperty(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
        }
        return jsonObject;
    }
}
