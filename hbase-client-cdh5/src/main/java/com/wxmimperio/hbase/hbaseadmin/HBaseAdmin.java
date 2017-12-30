package com.wxmimperio.hbase.hbaseadmin;

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HBaseAdmin {
    private static Logger LOG = LoggerFactory.getLogger(HBaseAdmin.class);

    private Connection connection;
    private static final String[] DEFAULT_COLUMN_FAMILY = {"c"};
    private static final String KEY_STRUCT = "key_struct";
    private static final String NAME_SPACE = "default";

    public HBaseAdmin(Connection connection) throws IOException {
        this.connection = connection;
    }

    public void createNameSpace(String nameSpaceName) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpaceName).build();
            admin.createNamespace(namespaceDescriptor);
        }
    }

    public void deleteNameSpace(String nameSpaceName) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            admin.deleteNamespace(nameSpaceName);
        }
    }

    public void createOrUpdateTable(String tableName, String[] cols, byte[][] splitKeys, JsonObject colAttributes, String keyStruct) throws IOException {
        TableName hTableName = TableName.valueOf(NAME_SPACE, tableName);
        try (Admin admin = connection.getAdmin()) {
            if (!admin.tableExists(hTableName)) {
                createTable(hTableName, cols, splitKeys, colAttributes, keyStruct);
                LOG.info("Table = " + tableName + " created.");
            } else {
                updateTable(hTableName, colAttributes, keyStruct);
                LOG.info("Table = " + tableName + " updated.");
            }
        }
    }

    public void createOrUpdateTable(String tableName, String[] cols, byte[][] splitKeys, String keyStruct) throws IOException {
        createOrUpdateTable(tableName, cols, splitKeys, new JsonObject(), keyStruct);
    }

    public void createOrUpdateTable(String tableName, byte[][] splitKeys, JsonObject colAttributes, String keyStruct) throws IOException {
        createOrUpdateTable(tableName, DEFAULT_COLUMN_FAMILY, splitKeys, colAttributes, keyStruct);
    }

    public void createOrUpdateTable(String tableName, String keyStruct) throws IOException {
        createOrUpdateTable(tableName, DEFAULT_COLUMN_FAMILY, new byte[][]{}, keyStruct);
    }

    private void createTable(TableName hTableName, String[] cols, byte[][] splitKeys, JsonObject colAttributes, String keyStruct)
            throws IOException {

        try (Admin admin = connection.getAdmin()) {
            // add table attributes
            HTableDescriptor hTableDescriptor = new HTableDescriptor(hTableName);
            if (!keyStruct.isEmpty()) {
                hTableDescriptor.setValue(KEY_STRUCT, keyStruct);
            }
            // add column family
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                if (colAttributes.getAsJsonObject().entrySet().size() != 0) {
                    Iterator<Map.Entry<String, JsonElement>> params = colAttributes.getAsJsonObject().entrySet().iterator();
                    while (params.hasNext()) {
                        Map.Entry<String, JsonElement> param = params.next();
                        hColumnDescriptor.setValue(Bytes.toBytes(param.getKey().toUpperCase()), Bytes.toBytes(param.getValue().getAsString()));
                    }
                }
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            if (splitKeys.length == 0) {
                admin.createTable(hTableDescriptor);
            } else {
                admin.createTable(hTableDescriptor, splitKeys);
            }
        }
    }

    private void disableTable(Admin admin, String tableName) throws IOException {
        TableName hTableName = TableName.valueOf(NAME_SPACE, tableName);
        if (admin.isTableEnabled(hTableName)) {
            admin.disableTable(hTableName);
        }
    }

    public void updateTable(TableName hTableName, JsonObject colAttributes, String keyStruct) throws IOException {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(hTableName);
        try (Admin admin = connection.getAdmin()) {
            if (!keyStruct.isEmpty()) {
                hTableDescriptor.setValue(KEY_STRUCT, keyStruct);
            }
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(DEFAULT_COLUMN_FAMILY[0]);
            if (colAttributes.getAsJsonObject().entrySet().size() != 0) {
                Iterator<Map.Entry<String, JsonElement>> params = colAttributes.getAsJsonObject().entrySet().iterator();
                while (params.hasNext()) {
                    Map.Entry<String, JsonElement> param = params.next();
                    hColumnDescriptor.setValue(Bytes.toBytes(param.getKey().toUpperCase()), Bytes.toBytes(param.getValue().getAsString()));
                }
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            disableTable(admin, hTableName.getNameAsString());
            admin.modifyTable(hTableName, hTableDescriptor);
            admin.enableTable(hTableName);
        }
    }

    public void deleteTable(String tableName) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            disableTable(admin, tableName);
            admin.deleteTable(TableName.valueOf(NAME_SPACE, tableName));
            LOG.info("Table = " + tableName + " deleted.");
        }
    }

    public boolean checkTableExist(String tableName) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            return admin.tableExists(TableName.valueOf(NAME_SPACE, tableName));
        }
    }

    public List<String> getKeyStruct(String tableName) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            HTableDescriptor desc = admin.getTableDescriptor(TableName.valueOf(tableName));
            String struct = desc.getValue(KEY_STRUCT);
            if (struct == null || struct.isEmpty()) {
                throw new IOException("Couldn't find key_struct for entity: " + tableName);
            } else {
                LOG.info("key_struct of " + tableName + " is " + struct);
            }
            return Lists.newArrayList(struct.split("\\|"));
        }
    }
}
