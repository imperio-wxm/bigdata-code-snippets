package org.example.reader;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MyParquetReader {
    private static final Logger LOG = LoggerFactory.getLogger(MyParquetReader.class);

    public static void read(String file) throws IOException {
        ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), new Path(file));
        try (ParquetReader<Group> reader = builder.build()) {
            SimpleGroup group = (SimpleGroup) reader.read();
            LOG.info("schema:" + group.getType().toString());
            LOG.info("idc_id:" + group.getString(1, 0));
        } finally {
            LOG.info(String.format("Read parquet file = %s", file));
        }
    }
}
