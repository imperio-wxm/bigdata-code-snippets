package com.wxmimperio.hdfs.utils;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;


public class HdfsUtils {
    private static final String HDFS_SITE = "/Users/weiximing/software/hadoop-2.8.3/etc/hadoop/hdfs-site.xml";
    private static final String CORE_SITE = "/Users/weiximing/software/hadoop-2.8.3/etc/hadoop/core-site.xml";
    private FileSystem fs;
    private static HdfsUtils hdfsUtils = null;

    public HdfsUtils() {
    }

    public static HdfsUtils getInstance() {
        if (hdfsUtils == null) {
            synchronized (HdfsUtils.class) {
                if (hdfsUtils == null) {
                    hdfsUtils = new HdfsUtils();
                }
            }
        }
        return hdfsUtils;
    }

    public FileSystem getFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.addResource(new Path(HDFS_SITE));
        conf.addResource(new Path(CORE_SITE));
        if (fs == null) {
            fs = FileSystem.newInstance(conf);

            System.out.println(fs.getConf().getFinalParameters());
        }
        return fs;
    }

    public FSDataOutputStream writeDataByFSOS(String filePath, List<String> data) throws IOException {
        if (isExists(filePath)) {
            System.out.println(String.format("File = %s exists...", filePath));
            return null;
        }
        FSDataOutputStream fsDataOutputStream = getFileSystem().create(new Path(filePath));
        for (String d : data) {
            fsDataOutputStream.write((d + "\n").getBytes());
            // 异步刷新，数据无法立即可见
            fsDataOutputStream.flush();
            // 同步刷新，数据立即可见
            // fsDataOutputStream.hflush();
        }
        return fsDataOutputStream;
    }

    public List<String> readData(String filePath) throws IOException {
        if (!isExists(filePath)) {
            System.out.println(String.format("File = %s not exists...", filePath));
            return null;
        }
        List<String> result = Lists.newArrayList();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(filePath))))) {
            result.addAll(reader.lines().collect(Collectors.toList()));
        }
        return result;
    }

    public boolean isExists(String filePath) throws IOException {
        Path path = new Path(filePath);
        if (getFileSystem().exists(path)) {
            return getFileSystem().isFile(path);
        } else {
            return false;
        }
    }

    public void readMerge(List<String> source, String sink) throws IOException {
        try (FSDataOutputStream fsDataOutputStream = getFileSystem().create(new Path(sink))) {
            for (int i = 0; i < source.size(); i++) {
                String s = source.get(i);
                for (String d : readData(s)) {
                    if (i == 3) {
                        // throw new RuntimeException("xxxx");
                        System.exit(1);
                    }
                    fsDataOutputStream.write((d + "\n").getBytes());
                    //fsDataOutputStream.hflush();
                }
            }
        }
    }

    public void copyMerge(List<String> source, String sink) throws IOException {
        if (isExists(sink)) {
            System.out.println(String.format("File = %s exists...", sink));
        }
        try (FSDataOutputStream fsOutStream = getFileSystem().create(new Path(sink), true)) {
            for (int i = 0; i < source.size(); i++) {
                String s = source.get(i);
                if (isExists(s)) {
                    try (FSDataInputStream inStream = getFileSystem().open(new Path(s))) {
                        if (i == 3) {
                            System.exit(1);
                            //throw new RuntimeException("xxxx");
                        }
                        IOUtils.copyBytes(inStream, fsOutStream, 4096, false);
                    }
                } else {
                    System.out.println(String.format("File = %s not exists.", s));
                }
            }
        }
    }

    public void closeFs() throws IOException {
        if (fs != null) {
            fs.close();
        }
    }
}
