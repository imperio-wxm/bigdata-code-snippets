package org.example;

import org.example.reader.MyParquetReader;
import org.example.utils.MyParquetUtils;
import org.example.writer.MyParquetWriter;

public class ParquetMain {

    public static void main(String[] args) throws Exception {
        String schemaStr = "message schema {" + "optional int64 log_id;"
                + "optional binary idc_id;" + "optional int64 house_id;"
                + "optional int64 src_ip_long;" + "optional int64 dest_ip_long;"
                + "optional int64 src_port;" + "optional int64 dest_port;"
                + "optional int32 protocol_type;" + "optional binary url64;"
                + "optional binary access_time;}";
        String file = "/Users/weiximing/code/github/bigdata-code-snippets/parquet-ops/src/main/resources/files/wxm.parquet";

        //MyParquetWriter.write(schemaStr, file);

        MyParquetReader.read(file);

        System.out.println(MyParquetUtils.getSchema(file).toString());
    }
}
