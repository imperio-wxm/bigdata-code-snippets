package com.wxmimperio.hdfs.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public interface FileSystemAccess {
    interface FileSystemExecutor<T> {
        T execute(FileSystem fs) throws IOException;
    }

    <T> T execute(String user, Configuration conf, FileSystemExecutor<T> executor);

    FileSystem createFileSystem(String user, Configuration conf) throws IOException;

    void releaseFileSystem(FileSystem fs) throws IOException;

    Configuration getFileSystemConfiguration();
}
