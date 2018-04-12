package com.wxmimperio.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class MapReduceFlowJob {

    public static class RowKeyMapper extends Mapper<Text, Text, Text, Text> {
        private static long vkey = 0L;
        private long mkey = 0L;

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            vkey = vkey + 1;
            mkey = vkey / 10000;
            context.write(new Text(String.valueOf(mkey)), value);
        }
    }

    public static class RowKeyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, new Text(value.toString() + "|" + key.toString()));
            }
        }
    }

    public static class FinalMapper extends Mapper<Text, Text, Text, Text> {
        private static long vkey = 0L;
        private long mkey = 0L;

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            vkey = vkey + 1;
            mkey = vkey / 10000;
            context.write(new Text(String.valueOf(mkey)), value);
        }
    }

    public static class FinalReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, new Text(value.toString() + "|" + key.toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("hdfs-site.xml");
        conf.addResource("core-site.xml");

        String inputPath = args[0];
        String rowKeyOutPutPath = args[1];
        String finalPath = args[2];

        // delete path
        HDFSUtils.delete(inputPath);
        HDFSUtils.delete(rowKeyOutPutPath);
        HDFSUtils.delete(finalPath);

        // RowKeyJob
        Job rowKeyJob = Job.getInstance(conf, "RowKeyJob");
        rowKeyJob.setJarByClass(MapReduceFlowJob.class);
        rowKeyJob.setMapperClass(RowKeyMapper.class);
        rowKeyJob.setCombinerClass(RowKeyReducer.class);
        rowKeyJob.setReducerClass(RowKeyReducer.class);
        rowKeyJob.setOutputKeyClass(Text.class);
        rowKeyJob.setOutputValueClass(Text.class);
        rowKeyJob.setInputFormatClass(TextInputFormat.class);
        rowKeyJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(rowKeyJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(rowKeyJob, new Path(rowKeyOutPutPath));

        // FinalJob
        Job finalJob = Job.getInstance(conf, "FinalJob");
        finalJob.setJarByClass(MapReduceFlowJob.class);
        finalJob.setMapperClass(FinalMapper.class);
        finalJob.setCombinerClass(FinalReducer.class);
        finalJob.setReducerClass(FinalReducer.class);
        finalJob.setOutputKeyClass(Text.class);
        finalJob.setOutputValueClass(Text.class);
        finalJob.setInputFormatClass(SequenceFileInputFormat.class);
        finalJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(finalJob, new Path(rowKeyOutPutPath));
        FileOutputFormat.setOutputPath(finalJob, new Path(finalPath));

        if (rowKeyJob.waitForCompletion(true)) {
            System.exit(finalJob.waitForCompletion(true) ? 0 : 1);
        }
    }
}
