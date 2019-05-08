package com.wxmimperio.kudu.client;

import com.wxmimperio.kudu.admin.KuduAdmin;
import com.wxmimperio.kudu.connection.KuduConnection;
import com.wxmimperio.kudu.exception.KuduClientException;
import com.wxmimperio.kudu.utils.ResourceUtils;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class KuduOpsClient {
    private static final Logger LOG = LoggerFactory.getLogger(KuduOpsClient.class);

    public static void main(String[] args) throws KuduClientException {
        KuduConnection kuduConnection = new KuduConnection(ResourceUtils.getByString("kudu.master"));
        try (KuduClient kuduClient = kuduConnection.getClient()) {

        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    private static int insertData(KuduClient kuduClient, String tableName) throws KuduException {
        if (KuduAdmin.checkTableExist(kuduClient, tableName)) {
            KuduTable table = kuduClient.openTable(tableName);
            KuduSession session = kuduClient.newSession();

            for (int i = 0; i < 10; i++) {
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addTimestamp("event_time", Timestamp.valueOf(LocalDateTime.now()));
                // Make even-keyed row have a null 'value'.
                if (i % 2 == 0) {
                    row.setNull("value");
                } else {
                    row.addString("value", "value " + i);
                }
                session.apply(insert);
            }
        } else {
            LOG.error(String.format("Table = %s not exists", tableName));
        }
    }
}
