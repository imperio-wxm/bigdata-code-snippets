package com.wxmimperio.hadoop.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by weiximing.imperio on 2017/2/16.
 */
public class OutputSequenceFile {
    //map
    public static class WCMapper extends Mapper<LongWritable, Text, Text, VLongWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] split = value.toString().split(":", 2);
            if (split.length != 1) {
                String[] splited = split[1].split(",");
                for (String s : splited) {
                    context.write(new Text(s), new VLongWritable(1L));
                }
            }
        }
    }

    //reduce
    public static class WCReducer extends Reducer<Text, VLongWritable, Text, VLongWritable> {
        @Override
        protected void reduce(Text key, Iterable<VLongWritable> v2s, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (VLongWritable vl : v2s) {
                sum += vl.get();
            }
            context.write(key, new VLongWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(OutputSequenceFile.class);

        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VLongWritable.class);

        // 设置输出类
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        /**
         * 设置sequecnfile的格式，对于sequencefile的输出格式，有多种组合方式,
         * 从下面的模式中选择一种，并将其余的注释掉
         */

        // 组合方式1：不压缩模式
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.NONE);

        //组合方式2：record压缩模式，并指定采用的压缩方式 ：默认、gzip压缩等
        /*SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.RECORD);
        SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);*/


        //组合方式3：block压缩模式，并指定采用的压缩方式 ：默认、gzip压缩等
       /* SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);*/

        FileInputFormat.addInputPaths(job, args[0]);
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
