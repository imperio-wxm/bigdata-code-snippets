package com.wxmimperio.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;


public class FileSystemUtil {
    private static Configuration conf = null;
    private static FileSystem fileSystem = null;
    private static String CORE_SITE_XML = "core-site.xml";
    private static String HDFS_SITE_XML = "hdfs-site.xml";

    static {
        conf = new Configuration();
        conf.addResource(CORE_SITE_XML);
        conf.addResource(HDFS_SITE_XML);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    }

    public static FileSystem getInstance() throws IOException {
        if (fileSystem == null) {
            synchronized (FileSystemUtil.class) {
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

}
