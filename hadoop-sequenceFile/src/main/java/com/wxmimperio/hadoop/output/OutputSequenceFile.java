package com.wxmimperio.hadoop.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by weiximing.imperio on 2017/2/16.
 */
public class OutputSequenceFile {
    //在map阶段接收输入的<key, value>(key是当前输入的行号，value是对应的内容)，
    //然后对内容进行切词，每切下一个词就将其组织成<word,1>的形式输出（输出即写到context中）
    //设置map的输入类型为<Object, Text>
    //输出类型为<Text, IntWritable>
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        //one表示单词出现1次
        private final static IntWritable one = new IntWritable(1);
        //word存储切下的单词
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //参数key表示的是行号，下面并没有用到key
            StringTokenizer itr = new StringTokenizer(value.toString()); //对输入的行进行切词
            while (itr.hasMoreTokens()) { //切下单词，存入word
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    //Reducer是对相同key下的所有value进行处理.
    //在reduce阶段，TaskTracker会接收到<word,{1,1,1,1}>形式的数据，也就是特定单词出现次数的情况
    //设置reduce的输入数据类型为<Text, IntWritable>
    //输出数据类型为<Text, IntWritable>
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        //result记录单词的个数
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            //对获取的<key, value-list>计算value的和
            for (IntWritable val : values) {
                sum += val.get();
            }
            //将频数设置到result中
            result.set(sum);
            //收集结果
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "E:\\software\\hadoop-2.6.0-cdh5.4.0");

        String inputPath = "hdfs://192.168.1.112:9000/wxm/input/read.txt";
        String outPath = "hdfs://192.168.1.112:9000/wxm/output/write";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(OutputSequenceFile.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输出类
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        /**
         * 设置sequecnfile的格式，对于sequencefile的输出格式，有多种组合方式,
         * 从下面的模式中选择一种，并将其余的注释掉
         */

        // 组合方式1：不压缩模式
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        //组合方式2：record压缩模式，并指定采用的压缩方式 ：默认、gzip压缩等
        /*SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.RECORD);
        SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);*/


        //组合方式3：block压缩模式，并指定采用的压缩方式 ：默认、gzip压缩等
       /* SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);*/

        FileInputFormat.addInputPaths(job, inputPath);
        SequenceFileOutputFormat.setOutputPath(job, new Path(outPath));

        job.waitForCompletion(true);
    }
}
