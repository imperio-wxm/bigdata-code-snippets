package com.wxmimperio.hbase.hbaseadmin;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


public class HbaseAdmin {
    private static Logger LOG = LoggerFactory.getLogger(HbaseAdmin.class);

    public static Connection connection;
    public static Admin admin;

    public HbaseAdmin() {
        initHbase();
    }

    private void initHbase() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            LOG.error("Get hbase connect error !", e);
        }
    }

    public void close() {
        try {
            if (null != admin)
                admin.close();
            if (null != connection)
                connection.close();
        } catch (IOException e) {
            LOG.error("Close hbase connect error !", e);
        }
    }

    public void createTable(String tableName, String[] cols) throws IOException {
        TableName hbaseTable = TableName.valueOf(tableName);
        if (admin.tableExists(hbaseTable)) {
            LOG.info("talbe is exists!");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(hbaseTable);
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
    }

    public void insertJsonRow(String tableName, String rowKey, String colFamily, List<JsonObject> jsonDatas) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        table.put(getPutListByJson(rowKey, colFamily, jsonDatas));
        table.close();
    }

    public static Table getTable(String tableName) throws IOException {
        return connection.getTable(TableName.valueOf(tableName));
    }

    public static String getTableRegionLocation(String tableName, String rowKey) throws IOException {
        Address regionServerAddress = connection.getRegionLocator(TableName.valueOf(tableName))
                .getRegionLocation(rowKey.getBytes()).getServerName().getAddress();
        return regionServerAddress.getHostname();
    }

    private List<Put> getPutListByJson(String rowKey, String colFamily, List<JsonObject> jsonDatas) {
        List<Put> putList = new ArrayList<Put>();
        for (JsonObject jsonData : jsonDatas) {
            Iterator<Map.Entry<String, JsonElement>> jsonIterator = jsonData.entrySet().iterator();
            while (jsonIterator.hasNext()) {
                Put put = new Put(Bytes.toBytes(rowKey));
                Map.Entry<String, JsonElement> jsonElement = jsonIterator.next();
                put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(jsonElement.getKey()), Bytes.toBytes(jsonElement.getValue().getAsString()));
                putList.add(put);
            }
        }
        return putList;
    }

    public void insterRow(String tableName, String rowKey, String colFamily, String col, String val) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        //table.put(put);
        List<Put> putList = new ArrayList<Put>();
        putList.add(put);
        table.put(putList);
        table.close();
    }

    public void scanData(String tableName, String startRow, String stopRow) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        /*System.out.println("startRow = " + startRow);
        System.out.println("stopRow = " + stopRow);*/
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));
        ResultScanner resultScanner = table.getScanner(scan);
        int i = 0;
        for (Result result : resultScanner) {
            showCell(result);
            i++;
        }
        System.out.println("all size = " + i);
        table.close();
    }

    public static void showCell(Result result) {
        JsonObject jsonObject = new JsonObject();
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            //System.out.println("RowName:" + new String(CellUtil.cloneFamily(cell)) + " ");
            /*System.out.println("Timetamp:" + cell.getTimestamp() + " ");
            System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");*/
            jsonObject.addProperty(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
        }
        System.out.println(jsonObject.toString());
    }
}
