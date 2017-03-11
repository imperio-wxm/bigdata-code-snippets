package com.wxmimperio.hadoop.wirter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;

/**
 * Created by wxmimperio on 2017/3/11.
 */
public class SequenceFileWrite {

    private static final String[] DATA = {
            "One, two, buckle my shoe",
            "Three, four, shut the door",
            "Five, six, pick up sticks",
            "Seven, eight, lay them straight",
            "Nine, ten, a big fat hen"
    };

    public static void main(String[] args) throws IOException {

        System.setProperty("hadoop.home.dir", "E:\\software\\hadoop-2.6.0-cdh5.4.0");

        // TODO Auto-generated method stub
        String uri = "hdfs://192.168.1.112:9000";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        String filePath = "/wxm/kafka";
        Path path = new Path(filePath);
        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
            int index = 0;
            for (String str : DATA) {
                key.set(index);
                value.set(str);
                writer.append(key, value);
                index++;
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }

}
