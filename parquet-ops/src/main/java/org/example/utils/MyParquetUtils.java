package org.example.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

public class MyParquetUtils {

    public static MessageType getSchema(String file) throws IOException {
        ParquetMetadata readFooter = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(file), new Configuration())).getFooter();
        return readFooter.getFileMetaData().getSchema();
    }
}
