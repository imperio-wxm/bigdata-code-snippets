package com.wxmimperio.kudu.admin;

import com.wxmimperio.kudu.connection.KuduConnection;
import com.wxmimperio.kudu.exception.KuduClientException;
import com.wxmimperio.kudu.utils.ResourceUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class KuduAdmin {

    private static final Logger LOG = LoggerFactory.getLogger(KuduAdmin.class);

    public static void main(String[] args) throws KuduClientException {
        KuduConnection kuduConnection = new KuduConnection(ResourceUtils.getByString("kudu.master"));
        try (KuduClient kuduClient = kuduConnection.getClient()) {
            String tableName = "kudu_test_wxm_0508_1";

            List<ColumnSchema> columns = new ArrayList<>(2);
            // pk
            columns.add(new ColumnSchema.ColumnSchemaBuilder("event_time", Type.INT64)
                    .key(true)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("_KEY", Type.INT64)
                    .key(true)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("game_id", Type.STRING).nullable(true)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("area_id", Type.STRING).nullable(true)
                    .build());

            // create table
            createTable(kuduClient, tableName, columns);

            // delete table
            //dropTable(kuduClient, tableName);

        } catch (KuduException e) {
            e.printStackTrace();
        }
    }


    private static void createTable(KuduClient kuduClient, String tableName, List<ColumnSchema> columns) throws KuduException {
        if (checkTableExist(kuduClient, tableName)) {
            LOG.error(String.format("Table = %s already exists", tableName));
        } else {
            Schema schema = new Schema(columns);
            CreateTableOptions cto = new CreateTableOptions();
            List<String> hashKeys = new ArrayList<>(2);
            hashKeys.add("event_time");
            hashKeys.add("_KEY");
            int numBuckets = 3;
            cto.addHashPartitions(hashKeys, numBuckets);
            cto.setNumReplicas(3);
            cto.setWait(true);
            kuduClient.createTable(tableName, schema, cto);
            LOG.info(String.format("Create table %s", tableName));
        }
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
}
