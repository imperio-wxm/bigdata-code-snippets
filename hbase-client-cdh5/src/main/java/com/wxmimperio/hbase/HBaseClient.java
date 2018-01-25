package com.wxmimperio.hbase;

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.wxmimperio.hbase.hbaseadmin.HBaseAdmin;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class HBaseClient {
    private static Logger LOG = LoggerFactory.getLogger(HBaseClient.class);

    private static final String DEFAULT_COLUMN_FAMILY = "c";
    private static final String NAME_SPACE = "default";
    private static final String EVENT_TIME = "event_time";
    private static final String MESSAGE_KEY = "message_key";
    private static final String ROW_KEY_DELIMITER = "|";
    private static final long DEFAULT_COUNT_LIMIT = 1000;
    private static final long DEFAULT_SIZE_LIMIT = 1 * 1000 * 1000;
    private static final long DEFAULT_MIN_TIMESTAMP = 0L;
    private static final long DEFAULT_MAX_TIMESTAMP = Long.MAX_VALUE;
    private static final String ROW_KEY = "rowkey";
    private static final String TIMESTAMP = "timestamp";
    private boolean showMetaData = false;
    private List<String> keyStruct;
    private TableName tableName;
    private Connection connection;
    private BufferedMutator mutator;
    private final ExecutorService workerPool = Executors.newFixedThreadPool(5);

    private static final ThreadLocal<SimpleDateFormat> eventTimeSdf = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    public HBaseClient(Connection connection, String tableName, String nameSpace) throws IOException {
        this.connection = connection;
        this.tableName = TableName.valueOf(HBaseAdmin.getNameSpace(nameSpace), tableName);
        this.keyStruct = new HBaseAdmin(connection).getKeyStruct(tableName, HBaseAdmin.getNameSpace(nameSpace));
        this.mutator = initBufferedMutator(connection, tableName, nameSpace);
    }

    private BufferedMutator initBufferedMutator(Connection connection, String tableName, String nameSpace) throws IOException {
        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    LOG.info("Failed to sent put " + e.getRow(i) + ".");
                }
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(HBaseAdmin.getNameSpace(nameSpace), tableName)).listener(listener);
        params.writeBufferSize(8 * 1024 * 1024);
        return connection.getBufferedMutator(params);
    }

    public void closeBufferedMutator() throws IOException {
        mutator.flush();
        mutator.close();
        workerPool.shutdown();
        try {
            while (!workerPool.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.info("Wait cache pool shutdown.....");
            }
        } catch (Exception e) {
            LOG.error("Shutdown cache pool error.", e);
        }
    }

    public List<JsonObject> getByCondition(JsonObject conditions, List<String> columns) throws Exception {
        String rowKey = extractRowKey(conditions);
        LOG.trace("getByCondition: conditions = {}, columns = {}, rowKey = {}",
                new Object[]{conditions, columns, rowKey}
        );
        return getByKeyList(Lists.newArrayList(rowKey), columns);
    }

    public List<JsonObject> getByKeyList(List<String> keyList, List<String> columns) throws IOException {
        return getByKeyList(keyList, columns, 0L, 0L);
    }

    public List<JsonObject> getByKeyList(List<String> keyList, List<String> columns, long minStamp, long maxStamp) throws IOException {
        LOG.trace("getByKeyList: keyList = {}, columns = {}, minStamp = {}, maxStamp = {}",
                new Object[]{keyList, columns, minStamp, maxStamp}
        );
        List<JsonObject> list = Lists.newArrayList();
        try (Table table = connection.getTable(tableName)) {
            TimeRange tr = getTimeRange(minStamp, maxStamp);
            List<Get> gets = Lists.newArrayList();
            for (String key : keyList) {
                Get get = new Get(key.getBytes()).setTimeRange(tr.getMin(), tr.getMax());
                for (String column : columns) {
                    get.addColumn(DEFAULT_COLUMN_FAMILY.getBytes(), column.getBytes());
                }
                gets.add(get);
            }
            for (Result result : table.get(gets)) {
                JsonObject json = convertResultToJson(result);
                if (json != null) {
                    list.add(json);
                }
            }
        }
        return list;
    }

    public List<JsonObject> getByKeyRange(String startKey, String stopKey, List<String> columns) throws IOException {
        return getByKeyRange(startKey, stopKey, columns, 0L, 0L);
    }

    public List<JsonObject> getByKeyRange(String startKey, String stopKey, List<String> columns, long minStamp, long maxStamp)
            throws IOException {
        LOG.trace("getByKeyRange: startKey = {}, stopKey = {}, columns = {}, minStamp = {}, maxStamp = {}",
                new Object[]{startKey, stopKey, columns, minStamp, maxStamp}
        );
        List<JsonObject> list = Lists.newArrayList();
        try (Table table = connection.getTable(tableName)) {
            Scan scan = new Scan();
            for (String column : columns) {
                scan.addColumn(DEFAULT_COLUMN_FAMILY.getBytes(), column.getBytes());
            }
            TimeRange tr = getTimeRange(minStamp, maxStamp);
            scan.setTimeRange(tr.getMin(), tr.getMax());
            if (startKey != null) {
                scan.setStartRow(Bytes.toBytes(startKey));
            }
            if (stopKey != null) {
                scan.setStopRow(Bytes.toBytes(stopKey));
            }
            try (ResultScanner rs = table.getScanner(scan)) {
                long count = 0;
                long size = 0;
                for (Result result = rs.next(); result != null; result = rs.next()) {
                    JsonObject json = convertResultToJson(result);
                    if (++count > DEFAULT_COUNT_LIMIT || (size += json.entrySet().size()) > DEFAULT_SIZE_LIMIT) {
                        break;
                    }
                    if (json != null) {
                        list.add(json);
                    }
                }
            }
        }
        return list;
    }

    public void put(String colFamily, List<JsonObject> dataList, boolean isUUIdRowKey, long ttl) throws Exception {
        LOG.trace("put: colFamily = {}, dataList = {}, isUUIdRowKey = {}, ttl = {}",
                new Object[]{colFamily, dataList, isUUIdRowKey, ttl}
        );
        List<Put> rows = convertJsonToPuts(colFamily, dataList, isUUIdRowKey, ttl);

        try (Table table = connection.getTable(tableName)) {
            long startPuts = System.currentTimeMillis();
            if (!rows.isEmpty()) {
                table.put(rows);
            }
            LOG.info(tableName.getNameAsString() + " put cost = " + (System.currentTimeMillis() - startPuts) + " ms");
        }
    }

    public void put(String colFamily, List<JsonObject> dataList, boolean isUUIdRowKey) throws Exception {
        put(colFamily, dataList, isUUIdRowKey, 0L);
    }

    public void put(List<JsonObject> dataList, boolean isUUIdRowKey, long ttl) throws Exception {
        put(DEFAULT_COLUMN_FAMILY, dataList, isUUIdRowKey, ttl);
    }

    public void put(List<JsonObject> dataList) throws Exception {
        put(DEFAULT_COLUMN_FAMILY, dataList, false);
    }

    public String getTableName() {
        return tableName.getNameAsString();
    }

    public void batchAsyncPut(List<JsonObject> jsonDatas) throws Exception {
        //long startRows = System.currentTimeMillis();
        List<Put> rows = convertJsonToPuts(DEFAULT_COLUMN_FAMILY, jsonDatas, true, 0);
        //LOG.info("format rows cost = " + (System.currentTimeMillis() - startRows) + " ms");
        if (!rows.isEmpty()) {
            long startPuts = System.currentTimeMillis();
            mutator.mutate(rows);
            LOG.info(Thread.currentThread().getName() + " "
                    + tableName.getNameAsString() + " put cost = " + (System.currentTimeMillis() - startPuts) + " ms");

            /*List<Future<Void>> futures = new ArrayList<>(1);

            futures.add(workerPool.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    long startPuts = System.currentTimeMillis();
                    mutator.mutate(rows);
                    LOG.info(Thread.currentThread().getName() + " "
                            + tableName.getNameAsString() +
                            " put cost = " + (System.currentTimeMillis() - startPuts) + " ms");
                    return null;
                }
            }));

            for (Future<Void> f : futures) {
                f.get(5, TimeUnit.MINUTES);
            }*/
            //mutator.flush();
            /*workerPool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        long startPuts = System.currentTimeMillis();
                        mutator.mutate(rows);
                        LOG.info(Thread.currentThread().getName() + " "
                                + tableName.getNameAsString() +
                                " put cost = " + (System.currentTimeMillis() - startPuts) + " ms");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });*/
        }
    }

    private TimeRange getTimeRange(long minStamp, long maxStamp) throws IOException {
        return new TimeRange(minStamp > 0L ? DEFAULT_MIN_TIMESTAMP : minStamp,
                minStamp > 0L ? DEFAULT_MAX_TIMESTAMP : maxStamp);
    }

    public List<Put> convertJsonToPuts(String colFamily, List<JsonObject> jsonData, boolean isUUidRowKey, long ttl)
            throws Exception {

        List<Put> putList = Lists.newArrayList();

        for (JsonObject data : jsonData) {
            long eventTimestamp = getTimestamp(data);
            String rowKey;
            if (isUUidRowKey) {
                rowKey = extractUUIdRowKey(data);
            } else {
                rowKey = extractRowKey(data);
            }
            Iterator<Map.Entry<String, JsonElement>> jsonIterator = data.entrySet().iterator();
            while (jsonIterator.hasNext()) {
                Map.Entry<String, JsonElement> jsonElement = jsonIterator.next();
                Put put = new Put(Bytes.toBytes(rowKey), eventTimestamp);
                String value = "";
                if (!jsonElement.getValue().isJsonNull()) {
                    value = jsonElement.getValue().getAsString();
                }
                put.addColumn(
                        Bytes.toBytes(colFamily),
                        Bytes.toBytes(jsonElement.getKey()),
                        Bytes.toBytes(value)
                );
                if (ttl > 0L) {
                    put.setTTL(ttl);
                    put.setDurability(Durability.ASYNC_WAL);
                }
                putList.add(put);
            }
        }
        return putList;
    }

    private long getTimestamp(JsonObject data) {
        long eventTimestamp = System.currentTimeMillis();
        if (data.has(EVENT_TIME)) {
            try {
                eventTimestamp = eventTimeSdf.get().parse(data.get(EVENT_TIME).getAsString()).getTime();
            } catch (ParseException e) {
            }
        } else {
            LOG.warn("This message can not get event_time = " + data.toString());
        }
        return eventTimestamp;
    }

    /**
     * 1. logicalKey = UUID取前12位 + event_time 取15:10:10 位精确到秒，避免UUID过短导致重复，确保event_time范围内唯一
     * 2. 实例：fe88b8082312151010
     *
     * @param data
     * @return
     */
    public static String extractUUIdRowKey(JsonObject data) throws Exception {
        String logicalKey;
        try {
            if (data.has(EVENT_TIME) && data.has(MESSAGE_KEY)) {
                logicalKey = HBaseClient.deleteCharString(data.get(MESSAGE_KEY).getAsString().substring(0, 13), '-')
                        + deleteCharString(data.get(EVENT_TIME).getAsString().substring(11, 19), ':');
            } else {
                logicalKey = HBaseClient.deleteCharString(UUID.randomUUID().toString().substring(0, 13), '-')
                        + String.valueOf(System.currentTimeMillis()).substring(4, 10);
            }
        } catch (Exception e) {
            throw new Exception("Event_time or message_key not qualified, data = " + data, e);
        }
        return logicalKey;
    }

    public static void main(String[] args) throws Exception {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("message_key", UUID.randomUUID().toString());
        jsonObject.addProperty("event_time", eventTimeSdf.get().format(new Date()));

        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            //extractUUIdRowKey(jsonObject);
        }

        System.out.println("cost = " + (System.currentTimeMillis() - start) / 1000.0);
    }

    /**
     * 1. rowkey = 建表时keyStruct中指定字段
     * 2. keyStruct 中字段必须在data消息体中
     * 3. 实例： pt_id|area_id
     *
     * @param data
     * @return
     * @throws Exception
     */
    private String extractRowKey(JsonObject data) throws Exception {
        StringBuilder rowKey = new StringBuilder();
        for (String keyPart : keyStruct) {
            if (data.has(keyPart)) {
                rowKey.append(data.get(keyPart).getAsString()).append(ROW_KEY_DELIMITER);
            } else {
                throw new Exception("Either Fields:" + keyStruct + " or rowKey are required in: " + tableName.getNameAsString());
            }
        }
        return rowKey.deleteCharAt(rowKey.length() - 1).toString();
    }

    private JsonObject convertResultToJson(Result result) {
        if (result == null || result.isEmpty()) {
            return null;
        }
        JsonObject data = new JsonObject();
        JsonObject timestamp = new JsonObject();
        for (Cell cell : result.rawCells()) {
            data.addProperty(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            timestamp.addProperty(Bytes.toString(CellUtil.cloneQualifier(cell)), cell.getTimestamp());
        }
        if (showMetaData) {
            data.addProperty(ROW_KEY, Bytes.toString(result.getRow()));
            data.add(TIMESTAMP, timestamp);
        }
        return data;
    }

    public void setShowMetaData(boolean showMetaData) {
        this.showMetaData = showMetaData;
    }

    public static String deleteCharString(String sourceString, char chElemData) {
        StringBuffer stringBuffer = new StringBuffer("");
        for (int i = 0; i < sourceString.length(); i++) {
            if (sourceString.charAt(i) != chElemData) {
                stringBuffer.append(sourceString.charAt(i));
            }
        }
        return stringBuffer.toString();
    }
}
