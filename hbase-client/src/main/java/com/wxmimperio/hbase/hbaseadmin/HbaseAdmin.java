package com.wxmimperio.hbase.hbaseadmin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


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
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            showCell(result);
        }
        table.close();
    }

    private void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("Timetamp:" + cell.getTimestamp() + " ");
            System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
        }
    }

    public void getData(String tableName, String rowKey, String columnName, String qulifier) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        get.addColumn(columnName.getBytes(), qulifier.getBytes());
        Result result = table.get(get);
        showCell(result);
        table.close();
    }

    public void deleteCell(String tableName, String rowKey, String columnName, String qulifier) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        delete.addColumn(columnName.getBytes(), qulifier.getBytes());
        table.delete(delete);
        table.close();
    }

    public boolean deleteTable(String tableName) throws IOException {
        TableName hbaseTable = TableName.valueOf(tableName);
        // disable table
        admin.disableTable(hbaseTable);
        // delete table
        admin.deleteTable(hbaseTable);
        if (admin.tableExists(hbaseTable)) {
            return false;
        }
        return true;
    }
}
