package com.wxmimperio.hadoop;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by weiximing.imperio on 2016/7/25.
 */
public class HDFSUtils {
    private static final Logger LOG = LoggerFactory.getLogger(HDFSUtils.class);


    private static String hdfsUri = "";
    private static final String DEFAULTFS = "fs.defaultFS";
    private static final String DFS_FAILURE_ENABLE = "dfs.client.block.write.replace-datanode-on-failure.enable";
    private static final String DFS_FAILURE_POLICY = "dfs.client.block.write.replace-datanode-on-failure.policy";
    private static final String DFS_TRANSFER_THREADS = "dfs.datanode.max.transfer.threads";
    private static final String DFS_SUPPORT_APPEND = "dfs.support.append";
    private static final String CORE_SITE_XML = "core-site.xml";
    private static final String HDFS_SITE_XML = "hdfs-site.xml";
    private static final String MAPRED_SITE_XML = "mapred-site.xml";
    private static final String YARN_SITE_XML = "mapred-site.xml";

    public HDFSUtils() {
    }

    public static Configuration config() {
        Configuration conf = new Configuration();
        conf.addResource(CORE_SITE_XML);
        conf.addResource(HDFS_SITE_XML);
        conf.addResource(MAPRED_SITE_XML);
        conf.addResource(YARN_SITE_XML);
        conf.set(DFS_SUPPORT_APPEND, "true");
        conf.set(DFS_FAILURE_ENABLE, "true");
        conf.set(DEFAULTFS, hdfsUri);
        conf.set(DFS_FAILURE_POLICY, "NEVER");
        conf.set(DFS_TRANSFER_THREADS, "16000");
        conf.set("dfs.client.block.write.locateFollowingBlock.retries", "20");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.setBoolean("fs.automatic.close", false);
        return conf;
    }


    public static boolean checkFileExist(String filePath) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsUri), config());
        return fs.exists(new Path(filePath));
    }

    public static void delete(String filePath) throws Exception {
        FileSystem fs = FileSystem.get(URI.create(hdfsUri), config());
        if (checkFileExist(filePath)) {
            fs.delete(new Path(filePath), true);
            LOG.info("Path = " + filePath + " delete success!");
        }
    }

    public static List<String> getFileList(String dataPath) {
        List<String> fileList = new ArrayList<String>();
        try {
            Path path = new Path(dataPath);
            FileSystem fs = FileSystem.get(URI.create(hdfsUri), config());
            FileStatus[] fileStatusArray = fs.globStatus(path);
            if (fileStatusArray != null) {
                for (FileStatus fileStatus : fileStatusArray) {
                    if (fs.isFile(fileStatus.getPath())) {
                        String fullPath = fileStatus.getPath().toString();
                        fileList.add(fullPath);
                    } else if (fs.isDirectory(fileStatus.getPath())) {
                        for (FileStatus fileStatus2 : fs.listStatus(fileStatus
                                .getPath())) {
                            if (fs.isFile(fileStatus2.getPath())) {
                                String fullPath = fileStatus2.getPath()
                                        .toString();
                                fileList.add(fullPath);
                            } else {
                                throw new Exception("file path error:");
                            }
                        }
                    }
                }
            } else {
            }
            return fileList;
        } catch (Exception e) {
            LOG.error("Get file list error." + dataPath, e);
        }
        return Lists.newArrayList();
    }

    public static void sequenceFileWriter(String filePath, List<String> buffer) {
        long startTime = System.currentTimeMillis();
        SequenceFile.Writer writer = null;
        try {
            Path sequencePath = new Path(filePath);
            writer = SequenceFile.createWriter(
                    config(),
                    SequenceFile.Writer.file(sequencePath),
                    SequenceFile.Writer.keyClass(Text.class),
                    SequenceFile.Writer.valueClass(Text.class),
                    // In hadoop-2.6.0-cdh5, it can use hadoop-common-2.6.5
                    // with appendIfExists()
                    SequenceFile.Writer.appendIfExists(true),
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK)
            );
            String msgValue = "";
            String msgKey = "";
            for (String data : buffer) {
                try {
                    String[] message = data.split("\\t", -1);
                    try {
                        msgKey = message[message.length - 1];
                    } catch (Exception e) {
                        msgKey = String.valueOf(System.currentTimeMillis());
                    }
                    String[] messageCopy = Arrays.copyOf(message, message.length - 1);
                    msgValue = StringUtils.join(Arrays.asList(messageCopy), "\t").toString();
                    writer.append(new Text((String) msgKey), new Text((String) msgValue));
                } catch (Exception e) {
                    LOG.error("Write data error.", e);
                }
            }
            writer.hflush();
        } catch (IOException e) {
            LOG.error("Create or Append SequenceFile happened error. File= " + filePath, e);
        } finally {
            IOUtils.closeStream(writer);
            LOG.info("Write to file =" + filePath + ",Each write cost = " + (System.currentTimeMillis() - startTime) + " ms, count = " + buffer.size());
        }
    }

    public static boolean isFileClosed(String filePath) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsUri), config());
        return ((DistributedFileSystem) fs).isFileClosed(new Path(filePath));
    }

    public static void renameFile(String oldFile, String newFile) {
        try {
            FileSystem hdfs = FileSystem.get(URI.create(hdfsUri), config());
            Path oldPath = new Path(oldFile);
            Path newPath = new Path(newFile);
            Path filePath = new Path(newFile.substring(0, newFile.lastIndexOf("/")));
            if (!hdfs.exists(filePath)) {
                hdfs.mkdirs(filePath);
                LOG.info("Create  path = " + filePath);
            }
            boolean isRename = hdfs.rename(oldPath, newPath);
            if (isRename) {
                LOG.info("oldFile " + oldPath + " to newFile " + newFile + " Success!");
            } else {
                LOG.error("oldFile " + oldPath + " to newFile " + newFile + " Error!");
            }
        } catch (Exception e) {
            LOG.error("Rename file = " + oldFile + " error!", e);
        }
    }
}
