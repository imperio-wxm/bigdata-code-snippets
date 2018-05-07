package com.wxmimperio.hadoop.tran;

import com.google.common.collect.Lists;
import com.wxmimperio.hadoop.utils.OrcUtils;
import com.wxmimperio.hadoop.writer.OrcFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;

public class SequenceToOrc {

    private static final Logger LOG = LoggerFactory.getLogger(SequenceToOrc.class);

    private static String hdfsUri = "";
    private static final String DFS_FAILURE_ENABLE = "dfs.client.block.write.replace-datanode-on-failure.enable";
    private static final String DFS_SUPPORT_APPEND = "dfs.support.append";
    private static final String CORE_SITE_XML = "core-site.xml";
    private static final String HDFS_SITE_XML = "hdfs-site.xml";

    static Configuration config() {
        Configuration conf = new Configuration();
        conf.addResource(CORE_SITE_XML);
        conf.addResource(HDFS_SITE_XML);
        conf.setBoolean(DFS_SUPPORT_APPEND, true);
        conf.setBoolean(DFS_FAILURE_ENABLE, true);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        return conf;
    }

    public static void getSequenceWriter(String sequenceFilePath, String outputPath, String db, String table) throws Exception {
        List<String> result = Lists.newArrayList();
        FileSystem fs = FileSystem.get(URI.create(hdfsUri), config());
        Path path = new Path(sequenceFilePath);

        TypeDescription schema = OrcUtils.getColumnTypeDescs(db, table);

        try (SequenceFile.Reader reader = new SequenceFile.Reader(config(), SequenceFile.Reader.file(path));
             Writer writer = OrcFileWriter.getOrcWriter(outputPath, schema)) {

            long index = 0L;
            if (fs.exists(path)) {
                Writable inKey = (Writable) ReflectionUtils.newInstance(
                        reader.getKeyClass(), config());
                Writable inValue = (Writable) ReflectionUtils.newInstance(
                        reader.getValueClass(), config());
                while (reader.next(inKey, inValue)) {
                    result.add(inValue.toString());
                    if (result.size() % 500 == 0) {
                        OrcFileWriter.writeData(writer, schema, result);
                        LOG.info("Write size  = " + result.size());
                        result.clear();
                    }
                    index++;
                }
                if (result.size() > 0) {
                    OrcFileWriter.writeData(writer, schema, result);
                    LOG.info("Final Write size  = " + result.size());
                    result.clear();
                }
            }
            LOG.info("File = " + sequenceFilePath + ", size = " + index);
        }
    }
}
