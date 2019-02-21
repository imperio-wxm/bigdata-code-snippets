package com.wxmimperio.hdfs.service;

import com.wxmimperio.hdfs.config.HdfsConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


@Component
public class FileSystemAccessService implements FileSystemAccess {

    private AtomicInteger unmanagedFileSystems = new AtomicInteger();
    private long purgeTimeout = 3 * 1000L;
    private Configuration conf;
    private static final String HTTPFS_FS_USER = "httpfs.fs.user";
    private HdfsConfig hdfsConfig;
    /**
     * FileSystem cache
     */
    private ConcurrentHashMap<String, CachedFileSystem> fsCache;

    @Autowired
    public FileSystemAccessService(HdfsConfig hdfsConfig) {
        this.hdfsConfig = hdfsConfig;
    }

    @PostConstruct
    private void initConfig() {
        Configuration configuration = new Configuration(false);
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.addResource(new Path(hdfsConfig.getHdfs()));
        configuration.addResource(new Path(hdfsConfig.getCore()));
        conf = configuration;
        fsCache = new ConcurrentHashMap<>(100000);
    }

    private static class CachedFileSystem {
        private FileSystem fs;
        private long lastUse;
        private long timeout;
        private int count;

        CachedFileSystem(long timeout) {
            this.timeout = timeout;
            lastUse = -1;
            count = 0;
        }

        synchronized FileSystem getFileSystem(Configuration conf)
                throws IOException {
            if (fs == null) {
                fs = FileSystem.get(conf);
            }
            lastUse = -1;
            count++;
            System.out.println("fs count = " + count);
            return fs;
        }

        synchronized void release() throws IOException {
            if (count >= 0) {
                count--;
            }
            if (count == 0) {
                if (timeout == 0) {
                    fs.close();
                    fs = null;
                    lastUse = -1;
                } else {
                    lastUse = System.currentTimeMillis();
                    System.out.println(lastUse);
                }
            }
            System.out.println("fs count = " + count);
        }

        // to avoid race conditions in the map cache adding removing entries
        // an entry in the cache remains forever, it just closes/opens filesystems
        // based on their utilization. Worse case scenario, the penalty we'll
        // pay is that the amount of entries in the cache will be the total
        // number of users in HDFS (which seems a resonable overhead).
        synchronized boolean purgeIfIdle() throws IOException {
            boolean ret = false;
            if (count == 0 && lastUse != -1 && (System.currentTimeMillis() - lastUse) > timeout) {
                fs.close();
                fs = null;
                lastUse = -1;
                ret = true;
            }
            return ret;
        }
    }

    @Scheduled(fixedRate = 30 * 1000)
    private void FileSystemCachePurger() {
        int count = 0;
        for (CachedFileSystem cacheFs : fsCache.values()) {
            try {
                count += cacheFs.purgeIfIdle() ? 1 : 0;
            } catch (Throwable ex) {
                System.out.println(("Error while purging filesystem, " + ex.toString() + ex.getMessage()));
            }
        }
        System.out.println("Purged [{}} filesystem instances" + count);
    }

    protected FileSystem createFileSystem(Configuration namenodeConf)
            throws IOException {
        String user = UserGroupInformation.getCurrentUser().getShortUserName();
        CachedFileSystem newCachedFS = new CachedFileSystem(purgeTimeout);
        CachedFileSystem cachedFS = fsCache.putIfAbsent(user, newCachedFS);
        if (cachedFS == null) {
            cachedFS = newCachedFS;
        }
        Configuration conf = new Configuration(namenodeConf);
        conf.set(HTTPFS_FS_USER, user);
        System.out.println("fsCache size = " + fsCache.size());
        return cachedFS.getFileSystem(conf);
    }


    protected void closeFileSystem(FileSystem fs) throws IOException {
        if (fsCache.containsKey(fs.getConf().get(HTTPFS_FS_USER))) {
            fsCache.get(fs.getConf().get(HTTPFS_FS_USER)).release();
            System.out.println("fsCache size = " + fsCache.size());
        }
    }

    @Override
    public <T> T execute(String user, Configuration conf, FileSystemExecutor<T> executor) {
        return null;
    }

    @Override
    public FileSystem createFileSystem(String user, Configuration configuration) throws IOException {
        return configuration == null ? createFileSystem(conf) : createFileSystem(configuration);
    }

    @Override
    public void releaseFileSystem(FileSystem fs) throws IOException {
        closeFileSystem(fs);
    }

    @Override
    public Configuration getFileSystemConfiguration() {
        return conf;
    }
}
