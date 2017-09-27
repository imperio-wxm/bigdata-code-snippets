package com.wxmimperio.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * Created by weiximing.imperio on 2017/9/27.
 */
public class HdfsHelper {


    private static final String DEFAULTFS = "fs.defaultFS";
    private static final String DFS_FAILURE_ENABLE = "dfs.client.block.write.replace-datanode-on-failure.enable";
    private static final String DFS_FAILURE_POLICY = "dfs.client.block.write.replace-datanode-on-failure.policy";
    private static final String DFS_SESSION_TIMEOUT = "dfs.client.socket-timeout";
    private static final String DFS_TRANSFER_THREADS = "dfs.datanode.max.transfer.threads";
    private static final String DFS_SUPPORT_APPEND = "dfs.support.append";
    private static final String CORE_SITE_XML = "core-site.xml";
    private static final String HDFS_SITE_XML = "hdfs-site.xml";
    private static final String MAPRED_SITE_XML = "mapred-site.xml";
    private static final String YARN_SITE_XML = "mapred-site.xml";


    static Configuration config() {
        Configuration conf = new Configuration();
        conf.addResource(CORE_SITE_XML);
        conf.addResource(HDFS_SITE_XML);
        conf.addResource(MAPRED_SITE_XML);
        conf.addResource(YARN_SITE_XML);
        conf.setBoolean(DFS_SUPPORT_APPEND, true);
        conf.setBoolean(DFS_FAILURE_ENABLE, true);
        conf.set(DEFAULTFS, "");
        conf.set(DFS_FAILURE_POLICY, "NEVER");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set(DFS_TRANSFER_THREADS, "16000");
        return conf;
    }


    private static SequenceFile.Writer getWriter(String sequenceFilePath) {
        SequenceFile.Writer writer = null;

        try {
            Path sequencePath = new Path(sequenceFilePath);
            Text values = new Text();
            Text key = new Text();
            writer = SequenceFile.createWriter(
                    config(),
                    SequenceFile.Writer.file(sequencePath),
                    SequenceFile.Writer.keyClass(key.getClass()),
                    SequenceFile.Writer.valueClass(values.getClass()),
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK)
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
        return writer;
    }


    public static void main(String[] args) {
        final SequenceFile.Writer writer = getWriter(args[0]);

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println("begin write-----");
                    for (int i = 0; i < 10; i++) {
                        Thread.sleep(1000);

                        Text msgValueText = new Text();
                        Text msgKeyText = new Text();

                        msgValueText.set(String.valueOf(i) + "_test");
                        msgKeyText.set(String.valueOf(i));
                        writer.append(msgKeyText, msgValueText);
                        writer.sync();
                        System.out.println("sleep " + i);
                    }
                    System.out.println("end write----- ");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("finish...");
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("closed...");
            }
        });
        t.start();
        addShutdownHook();
    }

    public static void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Received kill signal, stopping...");
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
