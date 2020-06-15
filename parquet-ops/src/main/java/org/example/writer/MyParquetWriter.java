package org.example.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class MyParquetWriter {
    private static final Logger LOG = LoggerFactory.getLogger(MyParquetWriter.class);

    public static void write(String schemaStr, String file) throws IOException {

        MessageType schema = MessageTypeParser.parseMessageType(schemaStr);
        /*
         * file, new GroupWriteSupport(), CompressionCodecName.SNAPPY, 256 *
         * 1024 * 1024, 1 * 1024 * 1024, 512, true, false,
         * ParquetProperties.WriterVersion.PARQUET_1_0, conf
         */
        ExampleParquetWriter.Builder builder = ExampleParquetWriter
                .builder(new Path(file)).withWriteMode(ParquetFileWriter.Mode.CREATE)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withConf(new Configuration())
                .withType(schema);

        try (ParquetWriter<Group> writer = builder.build()) {
            SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

            String[] access_log = {"111111", "22222", "33333", "44444", "55555", "666666", "777777", "888888", "999999", "101010"};
            for (int i = 0; i < 1000; i++) {
                writer.write(groupFactory.newGroup()
                        .append("log_id", Long.parseLong(access_log[0]))
                        .append("idc_id", access_log[1])
                        .append("house_id", Long.parseLong(access_log[2]))
                        .append("src_ip_long", Long.parseLong(access_log[3]))
                        .append("dest_ip_long", Long.parseLong(access_log[4]))
                        .append("src_port", Long.parseLong(access_log[5]))
                        .append("dest_port", Long.parseLong(access_log[6]))
                        .append("protocol_type", Integer.parseInt(access_log[7]))
                        .append("url64", access_log[8])
                        .append("access_time", access_log[9]));
            }
        } finally {
            LOG.info(String.format("Write parquet file to = %s", file));
        }
    }
}
