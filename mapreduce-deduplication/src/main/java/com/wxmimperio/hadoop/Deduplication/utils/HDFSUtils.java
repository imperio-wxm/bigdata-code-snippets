package com.wxmimperio.hadoop.Deduplication.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HDFSUtils {
    private static final Logger LOG = LoggerFactory.getLogger(HDFSUtils.class);

    public static final String EMPTY = "";
    private static Configuration conf = null;
    private static FileSystem fileSystem = null;
    private static String PREFIX = "/user/hive/warehouse/dw.db/";

    static {
        Properties prop = new Properties();
        try {
            prop.load(HDFSUtils.class.getClassLoader().getResourceAsStream("config.properties"));
            conf = new Configuration();
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            conf.addResource(new Path(prop.getProperty("core-site")));
            conf.addResource(new Path(prop.getProperty("hdfs-site")));
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static FileSystem getInstance() throws IOException {
        if (fileSystem == null) {
            synchronized (HDFSUtils.class) {
                if (fileSystem == null) {
                    fileSystem = FileSystem.get(conf);
                }
            }
        }
        return fileSystem;
    }

    public static FileSystem getByUri(URI uri) throws IOException {
        return FileSystem.get(uri, conf);
    }

    public static Configuration getConf() {
        return conf;
    }

    public static void deleteFiles(String filePath) throws Exception {
        FileSystem hdfs = HDFSUtils.getInstance();
        Path path = new Path(filePath);
        boolean isExists = hdfs.exists(path);
        if (isExists) {
            boolean isDel = hdfs.delete(path, true);
            LOG.info(filePath + "  delete? \t" + isDel);
        } else {
            LOG.error(filePath + "  exist? \t" + false);
        }
    }

    public static long getSize(String path) throws Exception {
        List<String> fileList = getFileList(path);
        FileSystem fs = HDFSUtils.getByUri(URI.create(path));
        long size = 0L;
        for (String file : fileList) {
            size += fs.getFileStatus(new Path(file)).getLen();
        }
        LOG.info("Get path = {}, all size = {}", path, size);
        return size;
    }

    public static String getInputPath(String tableName, String partDate) {
        return PREFIX + tableName + "/sequence/part_date=" + partDate;
    }

    public static String getOutputPath(String tableName, String partDate) {
        return PREFIX + tableName + "/de_orc/part_date=" + partDate;
    }

    public static String getRealPath(String tableName, String partDate) {
        return PREFIX + tableName + "/part_date=" + partDate;
    }

    public static String getMvPath(String path) {
        return path.replace("/de_orc", "");
    }

    public static boolean isFileClosed(String filePath) throws IOException {
        FileSystem fs = HDFSUtils.getInstance();
        if (!fs.exists(new Path(filePath))) {
            return false;
        }
        return ((DistributedFileSystem) fs).isFileClosed(new Path(filePath));
    }

    public static void renameFile(String oldFile, String newFile) {
        try {
            FileSystem hdfs = HDFSUtils.getInstance();
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

    public static List<String> getFileList(String dataPath) throws Exception {
        List<String> fileList = new ArrayList<>();
        try {
            Path path = new Path(dataPath);
            FileSystem fs = HDFSUtils.getInstance();
            FileStatus[] fileStatusArray = fs.globStatus(path);
            if (fileStatusArray != null) {
                for (FileStatus fileStatus : fileStatusArray) {
                    if (fs.isFile(fileStatus.getPath())) {
                        String fullPath = fileStatus.getPath().toString();
                        fileList.add(fullPath);
                    } else if (fs.isDirectory(fileStatus.getPath())) {
                        for (FileStatus fileStatus2 : fs.listStatus(fileStatus.getPath())) {
                            if (fs.isFile(fileStatus2.getPath())) {
                                String fullPath = fileStatus2.getPath().toString();
                                fileList.add(fullPath);
                            } else {
                                throw new Exception("file path error: " + fileStatus2.getPath().toString());
                            }
                        }
                    }
                }
            }
            return fileList;
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    public static void main(String[] args) {
        String str = "dfasd|fsdf|dfad";
        System.out.println(str.replaceAll("\\|","/"));
    }

    public static void formatFieldValue(SettableStructObjectInspector oi, StructField sf, OrcStruct orcStruct, String val) {
        WritableComparable wc = null;
        try {
            switch (sf.getFieldObjectInspector().getTypeName().toLowerCase()) {
                case "string":
                case "varchar":
                    wc = new Text(val);
                    break;
                case "bigint":
                    wc = new LongWritable(Long.valueOf(val));
                    break;
                case "int":
                    wc = new IntWritable(Integer.valueOf(val));
                    break;
                case "boolean":
                    wc = new BooleanWritable(Boolean.valueOf(val));
                    break;
                case "smallint":
                    wc = new ShortWritable(Short.valueOf(val));
                    break;
                case "float":
                    wc = new FloatWritable(Float.valueOf(val));
                    break;
                case "double":
                    wc = new DoubleWritable(Double.valueOf(val));
                    break;
                default:
                    break;
            }
            if (val.isEmpty()) {
                wc = null;
            }
        } catch (NumberFormatException e) {
            wc = null;
        }
        oi.setStructFieldData(orcStruct, sf, wc);
    }

}
