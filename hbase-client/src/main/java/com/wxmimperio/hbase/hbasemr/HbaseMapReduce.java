package com.wxmimperio.hbase.hbasemr;

import com.google.gson.JsonObject;
import com.wxmimperio.hbase.HTableInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;

public class HbaseMapReduce {
    private static Logger LOG = LoggerFactory.getLogger(HbaseMapReduce.class);

    public static class HBaseMapper extends TableMapper<ImmutableBytesWritable, Text> {
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            // process data for the row from the Result instance.
            context.write(new ImmutableBytesWritable("key".getBytes()), new Text(getJsonCell(value).toString()));
        }
    }

    public static class HbaseReduce extends Reducer<ImmutableBytesWritable, Text, Text, Text> {

        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text text : values) {
                context.write(new Text(), text);
            }
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String tableName = "test_table_1214";
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("htable.name", tableName);
        config.set("logical.scan.start", "1513250180083");
        config.set("logical.scan.stop", "1513250180222");
        config.set("start.null.slat", "0");
        config.set("end.null.slat", "10");
        LOG.info("Config = " + config);

        Job job = new Job(config, "HBaseMapReduceRead");
        job.setJarByClass(HbaseMapReduce.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        /*scan.setStartRow(Bytes.toBytes("200"));
        scan.setStopRow(Bytes.toBytes("300"));*/
        //HTableInputFormat.configureSplitTable(job, TableName.valueOf(tableName));

        TableMapReduceUtil.initTableMapperJob(
                tableName,
                scan,
                HBaseMapper.class,
                null,
                null,
                job);

        job.setInputFormatClass(HTableInputFormat.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(HbaseReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job, new Path("/wxm/hbase_test"));

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }

    public static JsonObject getJsonCell(Result value) {
        JsonObject jsonObject = new JsonObject();
        for (Cell cell : value.rawCells()) {
            jsonObject.addProperty("Rowkey", new String(CellUtil.cloneRow(cell)));
            jsonObject.addProperty(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
        }
        return jsonObject;
    }

}
