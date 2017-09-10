package com.wxmimperio.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * Created by wxmimperio on 2017/9/10.
 */
public class SequenceFileWriter implements RecordWriter {
    private static final Logger LOG = LoggerFactory.getLogger(SequenceFileWriter.class);


    private String filePath;
    private long fileCreateTimestamp = System.currentTimeMillis();
    private Configuration configuration;
    private SequenceFile.Writer writer;

    public SequenceFileWriter(String filePath, Configuration configuration) {
        this.filePath = filePath;
        this.configuration = configuration;
        try {
            initWriter();
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info("fileCreateTimestamp = " + fileCreateTimestamp);
    }

    @Override
    public void write(Object key, Object value) throws Exception {
        writer.append(new Text((String) key), new Text((String) value));
        writer.hflush();
    }

    @Override
    public void close() throws Exception {
        writer.hflush();
        writer.close();
        LOG.info("Writer " + filePath + " closed.");
    }

    private void initWriter() throws Exception {
        LOG.info("Start init writer " + filePath);
        Path sequencePath = new Path(filePath);
        writer = SequenceFile.createWriter(
                configuration,
                SequenceFile.Writer.file(sequencePath),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(Text.class),
                // In hadoop-2.6.0-cdh5, it can use hadoop-common-2.6.5
                // with appendIfExists()
                // SequenceFile.Writer.appendIfExists(true),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK)
        );
        LOG.info("End init writer " + filePath);
    }

    @Override
    public boolean shouldClosed() {
        return (System.currentTimeMillis() - fileCreateTimestamp) / 1000 > 10;
    }
}
