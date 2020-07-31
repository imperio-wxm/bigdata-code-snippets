package org.example;

import org.apache.parquet.schema.MessageType;
import org.example.reader.MyParquetReader;
import org.example.utils.MyParquetUtils;
import org.example.writer.MyParquetWriter;

import java.awt.*;

public class ParquetMain {

    public static void main(String[] args) throws Exception {
        String schemaStr = "message schema {" + "optional int64 log_id;"
                + "optional binary idc_id;" + "optional int64 house_id;"
                + "optional int64 src_ip_long;" + "optional int64 dest_ip_long;"
                + "optional int64 src_port;" + "optional int64 dest_port;"
                + "optional int32 protocol_type;" + "optional binary url64;"
                + "optional binary access_time;}";

        //String file = "/Users/weiximing/code/github/bigdata-code-snippets/parquet-ops/src/main/resources/files/part-99-26.lancer2.parquet";
        String file = "/Users/weiximing/code/github/bigdata-code-snippets/parquet-ops/src/main/resources/files/myschema.parquet";

        String nestedSchema =
                "message myschema {"
                        + " optional int64 log_id = 1;"
                        + " optional group app_info = 2 {"
                        + "         optional int32 app_id = 1;"
                        + "         optional int32 platform = 2;"
                        + " }"
                        + " optional group runtime_info = 3 {"
                        + "         optional int64 src_port = 1;"
                        + "         optional binary shared (STRING) = 2;"
                        + " }"
                        + " optional binary event_category (ENUM) = 4;"
                        + "}";

        //  optional group app_exposure_info = 12 {
        //    repeated group content_infos = 1 {
        //      optional binary event_id (STRING) = 1;
        //      repeated group extended_fields = 2 {
        //        optional binary key (STRING) = 1;
        //        optional binary value (STRING) = 2;
        //      }
        //    }
        //  }

        MyParquetWriter.write(nestedSchema, file);

        //MessageType type = MyParquetUtils.getSchema(file);

        //MyParquetReader.read(file, type);

        //System.out.println(type.toString());
        //MyParquetUtils.parserMessageType(type);
    }
}
