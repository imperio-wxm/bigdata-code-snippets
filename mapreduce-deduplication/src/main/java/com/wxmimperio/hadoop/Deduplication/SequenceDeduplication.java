package com.wxmimperio.hadoop.Deduplication;

import com.wxmimperio.hadoop.Deduplication.utils.HDFSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.wxmimperio.hadoop.Deduplication.utils.HDFSUtils.EMPTY;

/**
 * Created by weiximing.imperio on 2017/7/12.
 */
public class SequenceDeduplication {
    private static final Logger LOG = LoggerFactory.getLogger(SequenceDeduplication.class);
    private static final String MAPRED_OUTPUT_COMPRESS = "mapred.output.compress";
    private static final String CORE_SITE_XML = "core-site.xml";
    private static final String HDFS_SITE_XML = "hdfs-site.xml";

    public enum Count {
        TotalCount
    }

    public static class DeduplicationMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (key.toString().equalsIgnoreCase(EMPTY)) {
                context.write(value, new Text(EMPTY));
            } else {
                context.write((Text) key, value);
            }
        }
    }

    public static class DeduplicationReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text value = values.iterator().next();
            if (value.toString().equalsIgnoreCase(EMPTY)) {
                context.write(new Text(EMPTY), key);
                context.getCounter(Count.TotalCount).increment(1);
            } else {
                context.write(key, value);
                context.getCounter(Count.TotalCount).increment(1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HDFSUtils.getConf();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            // inputFormat：org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
            LOG.error("Usage: hadoop jar xxx.jar <inPath> <inputFormat> <outPath>");
            System.exit(1);
        }

        String inputPath = otherArgs[0];
        String outputPath = otherArgs[2];
        Class inputFormat = Class.forName(otherArgs[1]);

        // delete output directory
        deleteFile(conf, outputPath);

        int fileNum = HDFSUtils.getFileList(inputPath).size();
        if (fileNum <= 0) {
            LOG.error("Input = {} is empty, can not run job.", inputPath);
            System.exit(1);
        }
        long inputSize = HDFSUtils.getSize(inputPath);
        LOG.info("inputSize: {}", inputSize);
        int reduceNum;
        int splitNum = (int) (inputSize / (2048 * 1024 * 1024L));
        LOG.info("splitNum: {}", splitNum);
        int maxSplitNum = 100;
        if (splitNum > maxSplitNum) {
            reduceNum = 100;
        } else if (splitNum == 0) {
            reduceNum = 1;
        } else {
            reduceNum = splitNum;
        }
        LOG.info("reduce num = {}", reduceNum);

        Job job = Job.getInstance();
        job.setJarByClass(SequenceDeduplication.class);
        job.setMapperClass(DeduplicationMapper.class);
        job.setReducerClass(DeduplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(inputFormat);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        if (job.waitForCompletion(true)) {
            Counters counters = job.getCounters();
            Counter counter = counters.findCounter(Count.TotalCount);
            LOG.info("TotalCount = " + counter.getValue());
        }
    }

    private static void deleteFile(Configuration conf, String fileName) throws Exception {
        FileSystem hdfs = FileSystem.get(conf);
        Path path = new Path(fileName);
        boolean isExists = hdfs.exists(path);
        if (isExists) {
            boolean isDel = hdfs.delete(path, true);
            LOG.info(fileName + "  delete? \t" + isDel);
        } else {
            LOG.error(fileName + "  exist? \t" + false);
        }
    }
}
