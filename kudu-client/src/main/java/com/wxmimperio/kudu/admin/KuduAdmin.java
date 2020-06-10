package com.wxmimperio.kudu.admin;

import com.google.common.base.Charsets;
import com.wxmimperio.kudu.connection.KuduConnection;
import com.wxmimperio.kudu.exception.KuduClientException;
import com.wxmimperio.kudu.utils.AvroSchemaHelper;
import com.wxmimperio.kudu.utils.ResourceUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

public class KuduAdmin {

    private static final Logger LOG = LoggerFactory.getLogger(KuduAdmin.class);
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws KuduClientException, ParseException, IOException {
        KuduConnection kuduConnection = new KuduConnection(ResourceUtils.getByString("kudu.master"));
        try (KuduClient kuduClient = kuduConnection.getClient()) {
            String tableName = "test";

            // create table
            // createTable(kuduClient, tableName, columns);

            // delete table
            //dropTable(kuduClient, tableName);

            // createTableByAvroSchema
            //createTableByAvroSchema(kuduClient, tableName);

            // addRangePartition
            addRangePartition(kuduClient, tableName);

            // dropRangePartition
            //dropRangePartition(kuduClient, tableName);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }


    private static void createTable(KuduClient kuduClient, String tableName, List<ColumnSchema> columns) throws KuduException, ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        if (checkTableExist(kuduClient, tableName)) {
            LOG.error(String.format("Table = %s already exists", tableName));
        } else {
            Schema schema = new Schema(columns);
            CreateTableOptions cto = new CreateTableOptions();
            List<String> hashKeys = new ArrayList<>(2);
            //hashKeys.add("event_time");
            hashKeys.add("_KEY");
            int numBuckets = 3;
            cto.addHashPartitions(hashKeys, numBuckets);
            PartialRow startKey = schema.newPartialRow();
            startKey.addTimestamp("event_time", new Timestamp(simpleDateFormat.parse("2019-05-29").getTime()));
            PartialRow endKey = schema.newPartialRow();
            endKey.addTimestamp("event_time", new Timestamp(simpleDateFormat.parse("2019-05-30").getTime()));
            cto.addRangePartition(startKey, endKey);

            List<String> rangeKeys = new ArrayList<>();
            rangeKeys.add("event_time");
            cto.setRangePartitionColumns(rangeKeys);
            cto.setNumReplicas(3);
            cto.setWait(true);
            kuduClient.createTable(tableName, schema, cto);
            LOG.info(String.format("Create table %s", tableName));
        }
    }

    private static void createTableByAvroSchema(KuduClient kuduClient, String tableName) throws IOException, KuduClientException, ParseException {

        if (checkTableExist(kuduClient, tableName)) {
            LOG.error(String.format("Table = %s already exists", tableName));
        } else {
            Schema schema = new Schema(getKuduColByAvroSchema(AvroSchemaHelper.getAvroSchemaByName(tableName)));
            CreateTableOptions cto = new CreateTableOptions();
            List<String> hashKeys = new ArrayList<>(1);
            hashKeys.add("_key");
            int numBuckets = 3;
            cto.addHashPartitions(hashKeys, numBuckets);

            //cto.addRangePartition(startKey, endKey);

            PartialRow starKey = schema.newPartialRow();
            PartialRow endKey = schema.newPartialRow();
            starKey.addTimestamp("event_time", Timestamp.valueOf(formatter.format(formatter.parse("2019-06-02 00:00:00"))));
            endKey.addTimestamp("event_time", Timestamp.valueOf(formatter.format(formatter.parse("2019-06-03 00:00:00"))));
            cto.addRangePartition(starKey, endKey);

            List<String> rangeKeys = new ArrayList<>();
            rangeKeys.add("event_time");
            cto.setRangePartitionColumns(rangeKeys);
            cto.setNumReplicas(3);
            cto.setWait(true);
            kuduClient.createTable(tableName, schema, cto);
            LOG.info(String.format("Create table %s", tableName));
        }
    }

    private static void addRangePartition(KuduClient kuduClient, String tableName) throws KuduException {
        AlterTableOptions alterTableOptions = new AlterTableOptions();
        Schema schema = kuduClient.openTable(tableName).getSchema();
        PartialRow starKey = schema.newPartialRow();
        PartialRow endKey = schema.newPartialRow();
        starKey.addTimestamp("event_time", Timestamp.valueOf(formatter.format(formatter.parse("2019-06-11 00:00:00"))));
        endKey.addTimestamp("event_time", Timestamp.valueOf(formatter.format(formatter.parse("2019-06-12 00:00:00"))));
        alterTableOptions.addRangePartition(starKey, endKey);
        alterTableOptions.setWait(true);
        kuduClient.alterTable(tableName, alterTableOptions);
    }

    private static void dropRangePartition(KuduClient kuduClient, String tableName) throws KuduException {
        AlterTableOptions alterTableOptions = new AlterTableOptions();
        Schema schema = kuduClient.openTable(tableName).getSchema();
        PartialRow starKey = schema.newPartialRow();
        PartialRow endKey = schema.newPartialRow();
        starKey.addTimestamp("event_time", Timestamp.valueOf(formatter.format(formatter.parse("2019-05-21 00:00:00"))));
        endKey.addTimestamp("event_time", Timestamp.valueOf(formatter.format(formatter.parse("2019-05-22 00:00:00"))));
        alterTableOptions.dropRangePartition(starKey, endKey);
        alterTableOptions.setWait(true);
        kuduClient.alterTable(tableName, alterTableOptions);
    }

    public static boolean checkTableExist(KuduClient kuduClient, String tableName) throws KuduException {
        return kuduClient.tableExists(tableName);
    }

    private static void dropTable(KuduClient kuduClient, String tableName) throws KuduException {
        if (checkTableExist(kuduClient, tableName)) {
            kuduClient.deleteTable(tableName);
            LOG.info(String.format("Drop table %s", tableName));
        } else {
            LOG.error(String.format("Table = %s not exists", tableName));
        }
    }

    private static List<ColumnSchema> getKuduColByAvroSchema(org.apache.avro.Schema avroSchema) throws KuduClientException {
        List<ColumnSchema> columns = new ArrayList<>(avroSchema.getFields().size());
        for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
            for (org.apache.avro.Schema typeSchema : field.schema().getTypes()) {
                if (!typeSchema.isNullable()) {
                    switch (typeSchema.getType()) {
                        case STRING:
                            if ("event_time".equals(field.name())) {
                                columns.add(0, new ColumnSchema.ColumnSchemaBuilder(field.name(), Type.UNIXTIME_MICROS)
                                        .key(true)
                                        .encoding(ColumnSchema.Encoding.BIT_SHUFFLE)
                                        .compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY)
                                        .build()
                                );
                                columns.add(1, new ColumnSchema.ColumnSchemaBuilder("_key", Type.STRING).key(true).encoding(ColumnSchema.Encoding.PLAIN_ENCODING)
                                        .compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
                            } else {
                                columns.add(new ColumnSchema.ColumnSchemaBuilder(field.name(), Type.STRING).nullable(true).encoding(ColumnSchema.Encoding.PLAIN_ENCODING)
                                        .compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
                            }
                            break;
                        case BYTES:
                            columns.add(new ColumnSchema.ColumnSchemaBuilder(field.name(), Type.STRING).nullable(true).encoding(ColumnSchema.Encoding.PLAIN_ENCODING)
                                    .compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
                            break;
                        case INT:
                            columns.add(new ColumnSchema.ColumnSchemaBuilder(field.name(), Type.INT32).nullable(true).encoding(ColumnSchema.Encoding.BIT_SHUFFLE)
                                    .compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
                            break;
                        case LONG:
                            columns.add(new ColumnSchema.ColumnSchemaBuilder(field.name(), Type.INT64).nullable(true).encoding(ColumnSchema.Encoding.BIT_SHUFFLE)
                                    .compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
                            break;
                        case FLOAT:
                            columns.add(new ColumnSchema.ColumnSchemaBuilder(field.name(), Type.FLOAT).nullable(true).encoding(ColumnSchema.Encoding.BIT_SHUFFLE)
                                    .compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
                            break;
                        case DOUBLE:
                            columns.add(new ColumnSchema.ColumnSchemaBuilder(field.name(), Type.DOUBLE).nullable(true).encoding(ColumnSchema.Encoding.BIT_SHUFFLE)
                                    .compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
                            break;
                        case BOOLEAN:
                            columns.add(new ColumnSchema.ColumnSchemaBuilder(field.name(), Type.BOOL).nullable(true).encoding(ColumnSchema.Encoding.BIT_SHUFFLE)
                                    .compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
                            break;
                        default:
                            throw new KuduClientException(String.format("Not support this type = %s", typeSchema.getType()));
                    }
                }
            }
        }
        return columns;
    }
}
