package com.wxmimperio.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.HFilePrettyPrinter;
import org.apache.hadoop.hbase.io.hfile.HFileReaderV3;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.List;
import java.util.Map;


public class HFileReadMR {

    public static class HFileMapper extends Mapper<Object, Cell, ImmutableBytesWritable, Text> {
        @Override
        protected void map(Object key, Cell cell, Context context) throws IOException, InterruptedException {
            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(CellUtil.cloneRow(cell));
            String cvt = new String(CellUtil.cloneQualifier(cell)) + "|" + new String(CellUtil.cloneValue(cell)) + "|" + String.valueOf(cell.getTimestamp());
            context.write(rowKey, new Text(cvt));
        }
    }

    public static class HFileReducer extends Reducer<ImmutableBytesWritable, Text, NullWritable, Text> {
        @Override
        public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, List<String>> map = Maps.newHashMap();
            List<String> list = Lists.newArrayList();
            for (Text value : values) {
                String[] cvts = value.toString().split("\\|", -1);
                list.add(cvts[0]);
                list.add(cvts[1]);
                list.add(cvts[2]);
            }
            map.put(new String(key.toString()), list);
            context.write(NullWritable.get(), new Text(map.toString()));
        }
    }

    public static void main(String[] args) throws Exception {

        String inputPath = args[0];
        String outputPath = args[1];

        Configuration conf = HBaseConfiguration.create();
        conf.addResource("hbase-site.xml");
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");

        Job job = Job.getInstance(conf, "HFile MR");
        job.setJarByClass(HFileReadMR.class);

        job.setInputFormatClass(HFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(HFileMapper.class);
        //设置wordCountJob所用的reducer逻辑类为哪个类
        job.setReducerClass(HFileReducer.class);

        //设置map阶段输出的kv数据类型
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Text.class);

        //设置最终输出的kv数据类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(20);

        //设置要处理的文本数据所存放的路径
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        //提交job给hadoop集群
        job.waitForCompletion(true);
    }
}
