package com.wxmimperio.hadoop.Deduplication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by weiximing.imperio on 2017/7/12.
 */
public class FileDeduplication {
    private static final Logger LOG = LoggerFactory.getLogger(FileDeduplication.class);
    private static final String EMPTY = "";

    public static class DeduplicationMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, new Text(EMPTY));
        }
    }

    public static class DeduplicationReducer extends Reducer<Text, IntWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(new Text(EMPTY), key);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://sdg");
        conf.setBoolean("mapred.output.compress", false);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            LOG.error("Usage: java -cp xxx.jar com.xxx.FileDeduplication <inPath> <inputFormat> <outPath>");
            System.exit(2);
        }

        Class inputFormat = Class.forName(otherArgs[1]);

        // delete output directory
        deleteFile(conf, otherArgs[2]);

        Job job = Job.getInstance();
        job.setJarByClass(FileDeduplication.class);
        job.setMapperClass(DeduplicationMapper.class);
        job.setReducerClass(DeduplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(inputFormat);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        SequenceFileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void deleteFile(Configuration conf, String fileName) throws Exception {
        FileSystem hdfs = FileSystem.get(conf);
        Path path = new Path(fileName);
        boolean isExists = hdfs.exists(path);
        if (isExists) {
            boolean isDel = hdfs.delete(path, true);
            LOG.info(fileName + "  delete? \t" + isDel);
        } else {
            LOG.error(fileName + "  exist? \t" + isExists);
        }
    }
}
