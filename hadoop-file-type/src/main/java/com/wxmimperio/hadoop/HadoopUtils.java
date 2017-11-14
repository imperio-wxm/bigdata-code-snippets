package com.wxmimperio.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.orc.OrcFile;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HadoopUtils {
    private static Configuration conf = null;
    private static String CORE_SITE_XML = "core-site.xml";
    private static String HDFS_SITE_XML = "hdfs-site.xml";

    static {
        conf = new Configuration();
        conf.addResource(CORE_SITE_XML);
        conf.addResource(HDFS_SITE_XML);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    }


    public static boolean isSequenceFile(String uri) throws IOException, URISyntaxException {
        FileSystem fs = FileSystem.get(new URI(uri), conf);
        Path path = new Path(uri);
        SequenceFile.Reader reader = null;
        try {
            if (fs.getFileStatus(path).getLen() == 0)
                return false;
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            IOUtils.closeStream(reader);
        }
    }

    public static boolean isOrcFile(String uri) throws IOException {
        try {
            OrcFile.createReader(new Path(uri), OrcFile.readerOptions(conf));
            return true;
        } catch (IOException e) {
            return false;
        }
    }

}
