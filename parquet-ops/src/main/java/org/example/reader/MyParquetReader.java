package org.example.reader;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MyParquetReader {
    private static final Logger LOG = LoggerFactory.getLogger(MyParquetReader.class);

    public static void read(String file, MessageType messageType) throws IOException {
        ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), new Path(file));
        try (ParquetReader<Group> reader = builder.build()) {
            SimpleGroup group = (SimpleGroup) reader.read();

            for (int i = 0; i < messageType.getFieldCount(); i++) {
                Type type = messageType.getType(i);
                if (type.isPrimitive()) {
                    if (type.asPrimitiveType().getPrimitiveTypeName().name().equalsIgnoreCase("BINARY")) {
                        System.out.println("name = " + type.getName() + ",id = " + type.getId().intValue());
                        Binary binary = group.getBinary(type.getName(), 0);
                        System.out.println(binary.toStringUsingUTF8());
                        System.out.println("======");
                    }
                } else {
                    for (Type t : type.asGroupType().getFields()) {
                        System.out.println(t);
                    }
                }
            }

            /*System.out.println(group.getType().isPrimitive());
            System.out.println(group.getType().getId());
            System.out.println(group.getType().getName());
            //System.out.println(group.getType().getOriginalType().name());
            System.out.println(group);*/

            //LOG.info("idc_id:" + group.getString(1, 0));
        } finally {
            LOG.info(String.format("Read parquet file = %s", file));
        }
    }

    private static Object parserDate(Type type) {
        if(type.isPrimitive()) {
            switch (type.asPrimitiveType().getPrimitiveTypeName().name()) {
                case "BINARY":
                    break;
                case "INT":
                    break;
            }
        } else {

        }
        return null;
    }
}
