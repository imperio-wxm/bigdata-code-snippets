package com.wxmimperio.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;


public class FileSystemUtil {
    private static Configuration conf = null;
    private static FileSystem fileSystem = null;

    static {
        conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
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
