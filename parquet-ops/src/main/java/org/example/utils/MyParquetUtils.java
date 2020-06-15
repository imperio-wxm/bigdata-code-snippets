package org.example.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.util.Arrays;

public class MyParquetUtils {

    public static MessageType getSchema(String file) throws IOException {
        ParquetMetadata readFooter = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(file), new Configuration())).getFooter();
        return readFooter.getFileMetaData().getSchema();
    }

    public static void parserMessageType(MessageType messageType) {
        /*for (ColumnDescriptor columnDescriptor : messageType.getColumns()) {
            System.out.println(columnDescriptor);
            System.out.println(Arrays.asList(columnDescriptor.getPath()));
        }*/

        for (int i = 0; i < messageType.getFieldCount(); i++) {
            //System.out.println(messageType.getType(i));
            //System.out.println(messageType.getFields().get(i).getName());
        }
    }
}
