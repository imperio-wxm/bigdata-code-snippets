package com.wxmimperio.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HFileGenerator {

    private static List<String> columnNames;

    public static class HFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        private static final byte[] FAMILY_BYTE = Bytes.toBytes("c");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] items = line.split("\t", -1);
            long ptIdRowKey = Long.parseLong(items[0]);
            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(ptIdRowKey));
            for (int i = 0; i < items.length; i++) {
                Put put = new Put(rowKey.copyBytes());
                put.addColumn(FAMILY_BYTE, Bytes.toBytes(columnNames.get(i)), Bytes.toBytes(items[i]));
                context.write(rowKey, put);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        String tableName = args[0];
        columnNames = getColumnName(args[1]);
        String inputFile = args[2];
        String outputPath = args[3];

        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Job job = Job.getInstance(conf, "HFile Generator");
        job.setJarByClass(HFileGenerator.class);
        // set mapper class
        job.setMapperClass(HFileMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        // set input and output path
        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        // other config
        HFileOutputFormat2.configureIncrementalLoadMap(job, connection.getTable(TableName.valueOf(tableName)));
        // begin
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static List<String> getColumnName(String columnNames) {
        List<String> set = new ArrayList<String>();
        for (String columnName : columnNames.replaceAll("\\(", "").replaceAll("\\)", "").split(",", -1)) {
            set.add(columnName);
        }
        return set;
    }
}
