package com.wxmimperio.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by weiximing.imperio on 2017/6/5.
 */
public class DataWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DataWriter.class);

    private static final String DEFAULTFS = "fs.defaultFS";
    private static final String DFS_FAILURE_ENABLE = "dfs.client.block.write.replace-datanode-on-failure.enable";
    private static final String DFS_FAILURE_POLICY = "dfs.client.block.write.replace-datanode-on-failure.policy";
    private static final String DFS_TRANSFER_THREADS = "dfs.datanode.max.transfer.threads";
    private static final String DFS_SUPPORT_APPEND = "dfs.support.append";
    private static final String CORE_SITE_XML = "core-site.xml";
    private static final String HDFS_SITE_XML = "hdfs-site.xml";
    private static final String MAPRED_SITE_XML = "mapred-site.xml";
    private static final String YARN_SITE_XML = "mapred-site.xml";
    private final AtomicBoolean closed = new AtomicBoolean(false);


    private static Configuration config() {
        Configuration conf = new Configuration();
        conf.addResource(CORE_SITE_XML);
        conf.addResource(HDFS_SITE_XML);
        conf.addResource(MAPRED_SITE_XML);
        conf.addResource(YARN_SITE_XML);
        conf.setBoolean(DFS_SUPPORT_APPEND, true);
        conf.setBoolean(DFS_FAILURE_ENABLE, true);
        conf.set(DEFAULTFS, "hdfs://");
        conf.set(DFS_FAILURE_POLICY, "NEVER");
        conf.set(DFS_TRANSFER_THREADS, "16000");
        return conf;
    }

    public void sequenceFileWrite(String sequenceFilePath) {
        SequenceFile.Writer writer = null;
        try {
            // sequenceFile
            Path sequencePath = new Path(sequenceFilePath);

            Text value = new Text();
            BytesWritable EMPTY_KEY = new BytesWritable();
            BytesWritable key = new BytesWritable();
            writer = SequenceFile.createWriter(
                    config(),
                    SequenceFile.Writer.file(sequencePath),
                    SequenceFile.Writer.keyClass(key.getClass()),
                    SequenceFile.Writer.valueClass(value.getClass()),
                    //In hadoop-2.6.0-cdh5, it can use hadoop-common-2.6.5 with appendIfExists()
                    //SequenceFile.Writer.appendIfExists(true),
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK)
            );

            String line = "text";
            long index = 0L;
            while (!closed.get()) {
                value.set(line + "=" + index);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                writer.append(EMPTY_KEY, value);
                index++;
                System.out.println(value.toString());

                if (index % 2 == 0) {
                    writer.sync();
                    System.out.println("sync");
                }

                if (index == 30) {
                    closed.set(true);
                    System.out.println("close");
                }
            }
        } catch (RemoteException e) {
            LOG.warn("RemoteException happened warn. File= " + sequenceFilePath, e);
        } catch (IOException e) {
            LOG.error("Create or Append SequenceFile happened error. File= " + sequenceFilePath, e);
        } finally {
            IOUtils.closeStream(writer);
        }
    }

    /**
     * @param txtFilePath
     * @param sequenceFilePath
     */
    public void sequenceFileWrite(String txtFilePath, String sequenceFilePath) {
        SequenceFile.Writer writer = null;
        BufferedReader br = null;
        FileSystem fs = null;

        try {

            // txt
            Path txtPath = new Path(txtFilePath);
            fs = FileSystem.get(URI.create("hdfs://"), config());
            if (fs.exists(txtPath)) {
                // sequenceFile
                Path sequencePath = new Path(sequenceFilePath);
                Text value = new Text();
                BytesWritable EMPTY_KEY = new BytesWritable();
                BytesWritable key = new BytesWritable();
                writer = SequenceFile.createWriter(
                        config(),
                        SequenceFile.Writer.file(sequencePath),
                        SequenceFile.Writer.keyClass(key.getClass()),
                        SequenceFile.Writer.valueClass(value.getClass()),
                        //In hadoop-2.6.0-cdh5, it can use hadoop-common-2.6.5 with appendIfExists()
                        //SequenceFile.Writer.appendIfExists(true),
                        SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK)
                );

                FSDataInputStream inputStream = fs.open(txtPath);
                br = new BufferedReader(new InputStreamReader(inputStream));

                long startTime = System.currentTimeMillis();

                String line;
                long index = 0L;
                while (null != (line = br.readLine())) {
                    index++;
                    value.set(line);
                    writer.append(EMPTY_KEY, value);
                    writer.sync();
                }

                long endTime = System.currentTimeMillis();
                LOG.info("file " + txtPath + " size=" + index);
                LOG.info("cost = " + (endTime - startTime) + " ms");
            }
        } catch (RemoteException e) {
            LOG.warn("RemoteException happened warn. File= " + sequenceFilePath, e);
        } catch (IOException e) {
            LOG.error("Create or Append SequenceFile happened error. File= " + sequenceFilePath, e);
        } finally {
            IOUtils.closeStream(br);
            IOUtils.closeStream(writer);
        }
    }

    public static void main(String[] args) {
        /*Calendar calendar = Calendar.getInstance();
        int min = calendar.get(Calendar.MINUTE);
        new DataWriter().sequenceFileWrite("/tmp/wxm/dataWriter_" + min);*/

        //new DataWriter().sequenceFileWrite("/tmp/wxm/us_bpe_all_t_31.txt", "/tmp/wxm/dataWriter");
        new SimpleConsumer().start();
    }
}
