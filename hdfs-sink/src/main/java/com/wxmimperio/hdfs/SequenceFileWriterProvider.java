package com.wxmimperio.hdfs;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Created by wxmimperio on 2017/9/10.
 */
public class SequenceFileWriterProvider implements RecordWriterProvider {

    @Override
    public RecordWriter<String> getRecordWriter(Configuration conf, String fileName) throws IOException {
        return new SequenceFileWriter(fileName, conf);
    }
}
