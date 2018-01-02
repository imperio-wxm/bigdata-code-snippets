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
    private static final ThreadLocal<SimpleDateFormat> eventTimeSdf = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    public HBaseClient(Connection connection, String tableName) throws IOException {
        this.connection = connection;
        this.tableName = TableName.valueOf(NAME_SPACE, tableName);
        this.keyStruct = new HBaseAdmin(connection).getKeyStruct(tableName);
    }

    public List<JsonObject> getByCondition(JsonObject conditions, List<String> columns) throws Exception {
        String rowKey = extractRowKey(conditions);
        LOG.trace("getByCondition: conditions = {}, columns = {}, rowKey = {}",
                new Object[]{conditions, columns, rowKey}
        );
        return getByKeyList(Lists.newArrayList(rowKey), columns);
    }

    public List<JsonObject> getByKeyList(List<String> keyList, List<String> columns) throws IOException {
        return getByKeyList(keyList, columns, null, null);
    }

    public List<JsonObject> getByKeyList(List<String> keyList, List<String> columns, Long minStamp, Long maxStamp) throws IOException {
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
        return getByKeyRange(startKey, stopKey, columns, null, null);
    }

    public List<JsonObject> getByKeyRange(String startKey, String stopKey, List<String> columns, Long minStamp, Long maxStamp)
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

    public void put(String colFamily, List<JsonObject> dataList, boolean isUUIdRowKey, Long ttl) throws Exception {
        LOG.trace("put: colFamily = {}, dataList = {}, isUUIdRowKey = {}, ttl = {}",
                new Object[]{colFamily, dataList, isUUIdRowKey, ttl}
        );
        try (Table table = connection.getTable(tableName)) {
            List<Put> rows = convertJsonToPuts(colFamily, dataList, isUUIdRowKey, ttl);
            if (!rows.isEmpty()) {
                table.put(rows);
            }
        }
    }

    public void put(String colFamily, List<JsonObject> dataList, boolean isUUIdRowKey) throws Exception {
        put(colFamily, dataList, isUUIdRowKey, null);
    }

    public void put(List<JsonObject> dataList, boolean isUUIdRowKey, Long ttl) throws Exception {
        put(DEFAULT_COLUMN_FAMILY, dataList, isUUIdRowKey, ttl);
    }

    public void put(List<JsonObject> dataList) throws Exception {
        put(DEFAULT_COLUMN_FAMILY, dataList, false);
    }

    private TimeRange getTimeRange(Long minStamp, Long maxStamp) throws IOException {
        return new TimeRange(minStamp == null ? DEFAULT_MIN_TIMESTAMP : minStamp,
                maxStamp == null ? DEFAULT_MAX_TIMESTAMP : maxStamp);
    }

    private List<Put> convertJsonToPuts(String colFamily, List<JsonObject> jsonData, boolean isUUidRowKey, Long ttl)
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
                if (ttl != null && ttl > 0L) {
                    put.setTTL(ttl);
                }
                put.addColumn(
                        Bytes.toBytes(colFamily),
                        Bytes.toBytes(jsonElement.getKey()),
                        Bytes.toBytes(jsonElement.getValue().getAsString())
                );
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
     * 1. logicalKey = UUID取前8位 + timestamp（event_time）取6—10 位精确到秒，避免UUID过短导致重复，确保timestamp范围内唯一
     * 2. rowKey = 分桶数默认10，取第一位作为路由键
     * 3. 实例：4|fe88b8081213
     *
     * @param data
     * @return
     */
    private String extractUUIdRowKey(JsonObject data) throws Exception {
        String logicalKey = UUID.randomUUID().toString().substring(0, 8) + String.valueOf(System.currentTimeMillis()).substring(6, 10);
        try {
            if (data.has(EVENT_TIME) && data.has(MESSAGE_KEY)) {
                logicalKey = data.get(MESSAGE_KEY).getAsString().substring(0, 8) + String.valueOf(getTimestamp(data)).substring(6, 10);
            }
        } catch (Exception e) {
            throw new Exception("Event_time or message_key not qualified, data = " + data, e);
        }
        return Integer.toString(Math.abs(logicalKey.hashCode() % 10)).substring(0, 1) + ROW_KEY_DELIMITER + logicalKey;
    }

    /**
     * 1. logicalKey = UUID取前8位 + timestamp（event_time）取6—10 位精确到秒，避免UUID过短导致重复，确保timestamp范围内唯一
     * 2. rowKey = 分桶数默认10，取第一位作为路由键
     * 3. 实例：4|fe88b8081213
     *
     * @param data
     * @return
     */
    /*private String extractUUIdRowKey(JsonObject data) throws Exception {
        String logicalKey = UUID.randomUUID().toString() + new StringBuilder(String.valueOf(System.nanoTime())).reverse().substring(0, 4);
        try {
            if (data.has(EVENT_TIME) && data.has(MESSAGE_KEY)) {
                logicalKey = data.get(MESSAGE_KEY).getAsString();
            } else {
                LOG.info("This message can not get event_time and message_key, " + data.toString());
            }
        } catch (Exception e) {
            throw new Exception("Event_time or message_key not qualified, data = " + data, e);
        }
        return Integer.toString(Math.abs(logicalKey.hashCode() % 10)).substring(0, 1) + ROW_KEY_DELIMITER + logicalKey;
    }*/

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
}
