package com.wxmimperio.hbase.hbaseadmin;

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.wxmimperio.hbase.utils.HiveUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;


public class HbaseAdmin {
    private static Logger LOG = LoggerFactory.getLogger(HbaseAdmin.class);

    private static final long DEFAULT_COUNT_LIMIT = 1000;
    private static final long DEFAULT_SIZE_LIMIT = 1 * 1000 * 1000;

    protected static final byte[] DEFAULT_COLUMN_FAMILY = "cf1".getBytes();

    private static final long DEFAULT_MIN_TIMESTAMP = 0L;
    private static final long DEFAULT_MAX_TIMESTAMP = Long.MAX_VALUE;

    private static Connection connection;
    private static Admin admin;
    private static String HBASE_SITE = "hbaes-site.xml";

    public HbaseAdmin() {
        initHbase();
    }

    private void initHbase() {
        connection = getConnection();
        admin = getAdmin(connection);
    }

    private synchronized static Connection getConnection() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(HBASE_SITE);
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            LOG.error("Get hbase connect error !", e);
        }
        return connection;
    }

    private synchronized Admin getAdmin(Connection connection) {
        try {
            return connection.getAdmin();
        } catch (IOException e) {
            LOG.error("Get hbase admin error !", e);
        }
        return null;
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

    public void createTable(String tableName, String[] cols, byte[][] splitKeys) throws IOException {
        TableName hbaseTable = TableName.valueOf(tableName);
        if (admin.tableExists(hbaseTable)) {
            LOG.info("Table " + tableName + " exists!");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(hbaseTable);
            hTableDescriptor.setValue("key_struct", "{\"COMPRESSION\":\"SNAPPY\",\"VERSIONS\":\"1\"}");
            //hTableDescriptor.setValue("COMPRESSION","SNAPPY");
            //hTableDescriptor.setValue("VERSIONS","1");
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            if (splitKeys.length == 0) {
                admin.createTable(hTableDescriptor);
            } else {
                admin.createTable(hTableDescriptor, splitKeys);
            }
        }
    }

    public long batchAsyncPut(String tableName, String colFamily, List<Map<String, JsonObject>> puts) throws Exception {
        long currentTime = System.currentTimeMillis();
        Connection conn = getConnection();
        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    LOG.info("Failed to sent put " + e.getRow(i) + ".");
                }
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName))
                .listener(listener);
        params.writeBufferSize(5 * 1024 * 1024);

        final BufferedMutator mutator = conn.getBufferedMutator(params);
        try {
            mutator.mutate(getMutationListByJson(colFamily, puts));
            mutator.flush();
        } finally {
            mutator.close();
        }
        return System.currentTimeMillis() - currentTime;
    }

    public void insertJsonRow(String tableName, String colFamily, List<JsonObject> jsonDatas) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        table.put(getPutListByJson(colFamily, jsonDatas));
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

    /**
     * @param colFamily
     * @param jsonDatas
     * @return
     */
    private List<Mutation> getMutationListByJson(String colFamily, List<Map<String, JsonObject>> jsonDatas) {
        List<Mutation> putList = new ArrayList<Mutation>();
        for (Map<String, JsonObject> jsonMaps : jsonDatas) {
            for (Map.Entry<String, JsonObject> jsonMap : jsonMaps.entrySet()) {
                String rowKey = jsonMap.getKey();
                JsonObject data = jsonMap.getValue();
                Iterator<Map.Entry<String, JsonElement>> jsonIterator = data.entrySet().iterator();
                while (jsonIterator.hasNext()) {
                    Put put = new Put(Bytes.toBytes(rowKey));
                    Map.Entry<String, JsonElement> jsonElement = jsonIterator.next();
                    put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(jsonElement.getKey()), Bytes.toBytes(jsonElement.getValue().getAsString()));
                    putList.add(put);
                }
            }
        }
        return putList;
    }

    private List<Put> getPutListByJson(String colFamily, List<JsonObject> jsonDatas) throws ParseException {
        List<Put> putList = new ArrayList<Put>();
        for (JsonObject data : jsonDatas) {
            long eventTimestamp = System.currentTimeMillis();
            if (data.has("event_time")) {
                eventTimestamp = HiveUtil.eventTomeFormat.get().parse(
                        data.get("event_time").getAsString()
                ).getTime();
            } else {
                LOG.info("This message can not get event_time = " + data.toString());
            }
            /*String logicalKey = UUID.randomUUID().toString().substring(0, 8) + String.valueOf(eventTimestamp).substring(6, 10);
            String rowKey = Integer.toString(Math.abs(logicalKey.hashCode() % 10)).substring(0, 1) + "|" + logicalKey;*/

            String logicalKey = String.valueOf(System.currentTimeMillis()) + "|" + UUID.randomUUID().toString().substring(0, 8);
            String rowKey = Integer.toString(Math.abs(logicalKey.hashCode() % 10)).substring(0, 1) + "|" + logicalKey;

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Iterator<Map.Entry<String, JsonElement>> jsonIterator = data.entrySet().iterator();
            while (jsonIterator.hasNext()) {
                Map.Entry<String, JsonElement> jsonElement = jsonIterator.next();
                Put put = new Put(Bytes.toBytes(rowKey), eventTimestamp);
                put.setTTL(60 * 1000L * 60 * 24 * 7);
                put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(jsonElement.getKey()), Bytes.toBytes(jsonElement.getValue().getAsString()));
                putList.add(put);
            }
        }
        return putList;
    }

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 100; i++) {
            String logicalKey = UUID.randomUUID().toString().substring(0, 8);
            String rowKey = Integer.toString(Math.abs(logicalKey.hashCode() % 10)).substring(0, 1) + "|" + logicalKey;
            System.out.println(rowKey);
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

    public static JsonObject getJsonCell(Result result) {
        JsonObject jsonObject = new JsonObject();
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            jsonObject.addProperty(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
        }
        System.out.println(jsonObject.toString());
        return jsonObject;
    }

    public List<JsonObject> getData(String tableName, String rowKey, String family, String qualifier) {
        Table table = null;
        List<JsonObject> list = Lists.newArrayList();
        try {
            table = getConnection().getTable(TableName.valueOf(tableName));
            // 通过HBase中的 get来进行查询
            Get get = new Get(Bytes.toBytes(rowKey));
            // 如果列族不为空
            if (null != family && family.length() > 0) {
                // 如果列不为空
                if (null != qualifier && qualifier.length() > 0) {
                    get.addColumn(Bytes.toBytes(family),
                            Bytes.toBytes(qualifier));
                } else {
                    get.addFamily(Bytes.toBytes(family));
                }
            }
            Result result = table.get(get);
            List<Cell> cs = result.listCells();
            if (null == cs || cs.size() == 0) {
                return Lists.newArrayList();
            }
            JsonObject jsonObject = new JsonObject();
            for (Cell cell : cs) {
                jsonObject.addProperty("rowKey", Bytes.toString(CellUtil.cloneRow(cell)));// 取行健
                jsonObject.addProperty(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
            }
            list.add(jsonObject);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    public List<JsonObject> getByKeyList(String tableName, final List<String> keyList, final List<String> columns, Long minStamp, Long maxStamp) throws IOException {
        Table table = null;
        TimeRange tr = getTimeRange(minStamp, maxStamp);
        List<JsonObject> list = Lists.newArrayList();
        List<Get> gets = Lists.newArrayList();
        table = getConnection().getTable(TableName.valueOf(tableName));
        for (String key : keyList) {
            Get get = new Get(key.getBytes()).setTimeRange(tr.getMin(), tr.getMax());
            for (String column : columns) {
                get.addColumn(DEFAULT_COLUMN_FAMILY, column.getBytes());
            }
            gets.add(get);
        }
        for (Result result : table.get(gets)) {
            JsonObject json = getJsonCell(result);
            if (json != null) {
                list.add(json);
            }
        }
        return list;
    }

    private TimeRange getTimeRange(Long minStamp, Long maxStamp) throws IOException {
        return new TimeRange(minStamp == null ? DEFAULT_MIN_TIMESTAMP : minStamp,
                maxStamp == null ? DEFAULT_MAX_TIMESTAMP : maxStamp);
    }
}
