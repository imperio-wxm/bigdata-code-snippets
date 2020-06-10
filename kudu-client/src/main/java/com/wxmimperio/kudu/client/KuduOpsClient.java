package com.wxmimperio.kudu.client;

import com.wxmimperio.kudu.admin.KuduAdmin;
import com.wxmimperio.kudu.connection.KuduConnection;
import com.wxmimperio.kudu.exception.KuduClientException;
import com.wxmimperio.kudu.utils.AvroSchemaHelper;
import com.wxmimperio.kudu.utils.ResourceUtils;
import org.apache.avro.Schema;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.UUID;

public class KuduOpsClient {
    private static final Logger LOG = LoggerFactory.getLogger(KuduOpsClient.class);
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws KuduClientException, IOException {
        KuduConnection kuduConnection = new KuduConnection(ResourceUtils.getByString("kudu.master"));
        String tableName = "test";
        try (KuduClient kuduClient = kuduConnection.getClient()) {
            //insertData(kuduClient, tableName);

            // insertDataByAvroSchema
            insertDataByAvroSchema(kuduClient, tableName);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    private static int insertData(KuduClient kuduClient, String tableName) throws KuduException {
        if (KuduAdmin.checkTableExist(kuduClient, tableName)) {
            KuduTable table = kuduClient.openTable(tableName);
            KuduSession session = kuduClient.newSession();
            /*1、AUTO_FLUSH_SYNC（默认），意思是调用  KuduSession.apply() 方法后，客户端会在当数据刷新到服务器后再返回，这种情况就不能批量插入数据，调用  KuduSession.flush() 方法不会起任何作用，应为此时缓冲区数据已经被刷新到了服务器。

            2、AUTO_FLUSH_BACKGROUND，意思是调用  KuduSession.apply() 方法后，客户端会立即返回，但是写入将在后台发送，可能与来自同一会话的其他写入一起进行批处理。如果没有足够的缓冲空间，KuduSession.apply()会阻塞，缓冲空间不可用。因为写入操作是在后台应用进行的的，因此任何错误都将存储在一个会话本地缓冲区中。注意：这个模式可能会导致数据插入是乱序的，这是因为在这种模式下，多个写操作可以并发地发送到服务器。即此处为 kudu 自身的一个 bug,KUDU-1767 已经说明。

            3、MANUAL_FLUSH,意思是调用  KuduSession.apply() 方法后，会返回的非常快,但是写操作不会发送，直到用户使用flush()函数，如果缓冲区超过了配置的空间限制，KuduSession.apply()函数会返回一个错误。*/
            //session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
            Random random = new Random();
            for (int i = 0; i < 10000; i++) {
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addTimestamp("event_time", Timestamp.valueOf(formatter.format(LocalDateTime.now())));
                row.addLong("_KEY", UUID.randomUUID().getMostSignificantBits());
                row.addString("game_id", String.valueOf(random.nextInt(5000)));
                row.addString("area_id", String.valueOf(random.nextInt(5000)));
                session.apply(insert);
            }
            session.close();
        } else {
            LOG.error(String.format("Table = %s not exists", tableName));
        }
        return 0;
    }

    private static void insertDataByAvroSchema(KuduClient kuduClient, String tableName) throws KuduClientException, IOException {
        if (KuduAdmin.checkTableExist(kuduClient, tableName)) {
            KuduTable table = kuduClient.openTable(tableName);
            KuduSession session = kuduClient.newSession();
            session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
            Schema avroSchema = AvroSchemaHelper.getAvroSchemaByName(tableName);
            for (int i = 0; i < 10000; i++) {
                session.apply(getInsertByAvroSchema(avroSchema, table.newInsert()));
            }
            session.close();
        } else {
            LOG.error(String.format("Table = %s not exists", tableName));
        }
    }

    private static void deleteData(KuduClient kuduClient, String tableName) throws KuduException {
        if (KuduAdmin.checkTableExist(kuduClient, tableName)) {
            KuduTable table = kuduClient.openTable(tableName);
            Delete delete = table.newDelete();
            PartialRow row = delete.getRow();
            row.addInt("event_time", 0);
            KuduSession session = kuduClient.newSession();
            OperationResponse apply = session.apply(delete);
            if (apply.hasRowError()) {
                apply.getRowError();
            }
            session.close();
        }
    }

    private static Insert getInsertByAvroSchema(Schema avroSchema, Insert insert) throws KuduClientException {
        Random random = new Random();
        PartialRow row = insert.getRow();
        for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
            for (org.apache.avro.Schema typeSchema : field.schema().getTypes()) {
                if (!typeSchema.isNullable()) {
                    switch (typeSchema.getType()) {
                        case STRING:
                            if ("event_time".equals(field.name())) {
                                row.addTimestamp(field.name(), Timestamp.valueOf(formatter.format(formatter.parse("2019-05-30 00:04:00"))));
                                row.addString("_key", UUID.randomUUID().toString());
                            } else {
                                row.addString(field.name(), UUID.randomUUID().toString());
                            }
                            break;
                        case BYTES:
                            row.addString(field.name(), UUID.randomUUID().toString());
                            break;
                        case INT:
                            row.addInt(field.name(), random.nextInt(5000));
                            break;
                        case LONG:
                            row.addLong(field.name(), random.nextInt(5000));
                            break;
                        case FLOAT:
                            row.addFloat(field.name(), random.nextInt(5000) * 0.1F);
                            break;
                        case DOUBLE:
                            row.addDouble(field.name(), random.nextInt(5000) * 0.1);
                            break;
                        case BOOLEAN:
                            row.addBoolean(field.name(), true);
                            break;
                        default:
                            throw new KuduClientException(String.format("Not support this type = %s", typeSchema.getType()));
                    }
                }
            }
        }
        return insert;
    }

}
