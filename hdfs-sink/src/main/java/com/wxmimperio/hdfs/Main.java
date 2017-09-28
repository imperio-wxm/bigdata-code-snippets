package com.wxmimperio.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by wxmimperio on 2017/9/10.
 */
public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static Map<String, RecordWriter> sequenceFileWriterMap = new HashMap<String, RecordWriter>();

    private static final String DEFAULTFS = "fs.defaultFS";
    private static final String DFS_FAILURE_ENABLE = "dfs.client.block.write.replace-datanode-on-failure.enable";
    private static final String DFS_FAILURE_POLICY = "dfs.client.block.write.replace-datanode-on-failure.policy";
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
        conf.set(DFS_SUPPORT_APPEND, "true");
        conf.set(DFS_FAILURE_ENABLE, "true");
        conf.set(DEFAULTFS, "");
        conf.set(DFS_FAILURE_POLICY, "NEVER");
        conf.set(DFS_TRANSFER_THREADS, "16000");
        return conf;
    }

    private static boolean checkFileExist(String filePath) throws Exception {
        FileSystem fs = FileSystem.get(URI.create(""), config());
        return fs.exists(new Path(filePath));
    }

    public static void main(String[] args) {

        ExecutorService executorService = Executors.newFixedThreadPool(50);

        boolean flags = true;
        String fileName = "/wxm/test/test_1";
        RecordWriterProvider recordWriterProvider = new SequenceFileWriterProvider();
        try {
            RecordWriter recordWriter = null;
            if (checkFileExist(fileName)) {
                fileName = fileName + "_" + System.currentTimeMillis();
                recordWriter = recordWriterProvider.getRecordWriter(config(), fileName);
            }

            if (recordWriter != null) {
                sequenceFileWriterMap.put(fileName, recordWriter);
            }

            int index = 0;
            while (flags) {
                executorService.submit(new WriteTask(recordWriter, String.valueOf(index), String.valueOf(index)));
                index++;

                if (index > 1000000) {
                    break;
                }
                if (recordWriter.shouldClosed()) {
                    recordWriter.close();
                    sequenceFileWriterMap.remove(fileName);
                    LOG.info("key=" + index + " value" + index);

                }
                Thread.sleep(100);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
