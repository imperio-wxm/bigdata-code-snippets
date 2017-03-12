package com.wxmimperio.hadoop.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by wxmimperio on 2017/3/11.
 */
public class SequenceFileWrite {

    private static final String[] DATA = {
            "One, two, buckle my shoe",
            "Three, four, shut the door",
            "Five, six, pick up sticks",
            "Seven, eight, lay them straight",
            "Nine, ten, a big fat hen",
    };

    private String filePath;

    public SequenceFileWrite(String filePath) {
        this.filePath = filePath;
    }

    public void run() {
        SequenceFile.Writer writer = null;

        try {
            // TODO Auto-generated method stub
            String uri = "hdfs://192.168.1.112:9000";

            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", uri);
            conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
            conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");

            //FileSystem fs =  FileSystem.get(URI.create(uri), conf);

            Path path = new Path(filePath);
            BytesWritable key = new BytesWritable();
            Text value = new Text();
            BytesWritable EMPTY_KEY = new BytesWritable();

            //FSDataOutputStream output = fs.append(path);

            writer = SequenceFile.createWriter(
                    conf,
                    SequenceFile.Writer.file(path),
                    SequenceFile.Writer.keyClass(key.getClass()),
                    SequenceFile.Writer.valueClass(value.getClass()),
                    SequenceFile.Writer.appendIfExists(true),
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK)
            );

            int index = 0;
            for (String str : DATA) {
                //key.set(EMPTY_KEY);
                value.set(str);
                writer.append(EMPTY_KEY, value);
                index++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(writer);
        }
    }

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "E:\\software\\hadoop-2.6.0-cdh5.4.0");

        String filePath = "/wxm/kafka";

        for (int i = 0; i < 10; i++) {
            new SequenceFileWrite(filePath).run();
        }
    }
}
