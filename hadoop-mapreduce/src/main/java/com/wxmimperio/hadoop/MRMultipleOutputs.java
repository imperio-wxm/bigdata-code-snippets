package com.wxmimperio.hadoop;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class MRMultipleOutputs {

    public static class ReadMapper extends Mapper<Object, Text, Text, Text> {
        private static long vkey = 0L;
        private long mkey = 0L;

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            vkey = vkey + 1;
            mkey = vkey / 10000;
            context.write(new Text(String.valueOf(mkey)), value);
        }
    }

    public static class MultipleOutputsReducer extends Reducer<Text, Text, Text, Text> {
        private MultipleOutputs<Text, Text> mos;
        private static ThreadLocal<DateFormat> simpleDateFormat = new ThreadLocal<DateFormat>() {
            @Override
            protected DateFormat initialValue() {
                return new SimpleDateFormat("yyyy-MM-dd");
            }
        };

        private static ThreadLocal<DateFormat> eventTime = new ThreadLocal<DateFormat>() {
            @Override
            protected DateFormat initialValue() {
                return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }
        };

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            mos = new MultipleOutputs<>(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            mos.close();
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String msg = value.toString();
                if (StringUtils.isNotEmpty(msg)) {
                    String[] msgs = msg.split("\\|", -1);
                    if (msgs.length > 0) {
                        String tableName = msgs[0];
                        String partDate = simpleDateFormat.get().format(new Date());
                        try {
                            simpleDateFormat.get().format(eventTime.get().parse(msgs[1]));
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        System.out.println(msg);
                        mos.write(key, value, tableName + "/part_date=" + partDate + "/" + tableName + "_" + UUID.randomUUID().toString().replaceAll("-", ""));
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.addResource("hdfs-site.xml");
        conf.addResource("core-site.xml");

        try {
            String inputPath = args[0];
            String outPutPath = args[1];
            String remotePath = args[2];

            HDFSUtils.copyFile(inputPath, remotePath);

            Job job = Job.getInstance(conf, "Transform Data");
            job.setJarByClass(MRMultipleOutputs.class);
            job.setMapperClass(ReadMapper.class);
            job.setReducerClass(MultipleOutputsReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path(remotePath));
            FileOutputFormat.setOutputPath(job, new Path(outPutPath));

            if (job.waitForCompletion(true)) {
                System.exit(job.waitForCompletion(true) ? 0 : 1);
            }
        } catch (Exception e) {
            System.exit(1);
        }
    }
}
