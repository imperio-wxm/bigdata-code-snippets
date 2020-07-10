package com.wxmimperio.hdfs;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.wxmimperio.hdfs.utils.HdfsUtils;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MergeMain {
    private static final int fileNum = 5;
    private static final String basePath = "/flume/bigdata/wxm_test";

    public static void main(String[] args) throws IOException {
        HdfsUtils hdfsUtils = HdfsUtils.getInstance();

        //testWrite(hdfsUtils);
        testCopyMerge(hdfsUtils);
        //testReadMerge(hdfsUtils);

        //read(getPath(fileNum), hdfsUtils);

        hdfsUtils.closeFs();
    }

    public static void read(List<String> source, HdfsUtils hdfsUtils) throws IOException {
        for (String s : source) {
            System.out.println(hdfsUtils.readData(s));
        }
    }

    public static void testReadMerge(HdfsUtils hdfsUtils) throws IOException {
        String sinkPath = basePath + "/merge/readMerge";
        hdfsUtils.readMerge(getPath(fileNum), sinkPath);
    }

    public static void testCopyMerge(HdfsUtils hdfsUtils) throws IOException {
        String sinkPath = basePath + "/merge/mergeFile";
        hdfsUtils.copyMerge(getPath(fileNum), sinkPath);
    }

    public static void testWrite(HdfsUtils hdfsUtils) throws IOException {
        Map<String, FSDataOutputStream> writeMap = Maps.newHashMap();

        for (String realPath : getPath(fileNum)) {
            if (!writeMap.containsKey(realPath)) {
                FSDataOutputStream fsDataOutputStream = hdfsUtils.writeDataByFSOS(realPath, getData());
                if (fsDataOutputStream != null) {
                    writeMap.put(realPath, fsDataOutputStream);
                }
            }
        }

        writeMap.forEach((key, value) -> {
            try {
                System.out.println(hdfsUtils.readData(key));
                value.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public static List<String> getPath(int fileNum) {
        List<String> list = Lists.newArrayList();
        for (int i = 0; i < fileNum; i++) {
            String realPath = basePath + "/file_" + i;
            list.add(realPath);
        }
        return list;
    }

    public static List<String> getData() {
        List<String> list = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            list.add("test data = " + i);
        }
        return list;
    }
}
