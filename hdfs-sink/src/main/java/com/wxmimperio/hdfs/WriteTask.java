package com.wxmimperio.hdfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wxmimperio on 2017/9/10.
 */
public class WriteTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(WriteTask.class);


    private RecordWriter recordWriter;
    private String key;
    private String value;


    public WriteTask(RecordWriter recordWriter, String key, String value) {
        this.recordWriter = recordWriter;
        this.key = key;
        this.value = value;
    }

    @Override
    public void run() {
        try {
            recordWriter.write("key=" + key, "value" + value);
            LOG.info("key=" + key + " value" + value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
