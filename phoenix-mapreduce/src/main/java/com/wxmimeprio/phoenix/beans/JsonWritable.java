package com.wxmimeprio.phoenix.beans;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.*;

public class JsonWritable implements DBWritable, Writable {

    private JsonArray data;

    @Override
    public void readFields(DataInput input) throws IOException {

    }

    @Override
    public void write(DataOutput output) throws IOException {

    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        ResultSetMetaData resultSetMetaData = rs.getMetaData();
        int colCnt = resultSetMetaData.getColumnCount();
        data = new JsonArray();
        for (int i = 1; i <= colCnt; i++) {
            JsonObject jsonObject = new JsonObject();
            String type = resultSetMetaData.getColumnTypeName(i);
            jsonObject.addProperty("type", type);
            jsonObject.addProperty("value", rs.getString(resultSetMetaData.getColumnName(i)));
            data.add(jsonObject);
        }
    }

    @Override
    public void write(PreparedStatement pstmt) throws SQLException {
        int j = 1;
        for (JsonElement jsonElement : data) {
            Object value = parseType(jsonElement);
            if (value == null) {
                pstmt.setNull(j, Types.NULL);
            } else {
                pstmt.setObject(j, value);
            }
            j++;
        }
    }

    private Object parseType(JsonElement jsonElement) {
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        String value = jsonObject.get("value").isJsonNull() ? null : jsonObject.get("value").getAsString();
        if (value == null || "null".equalsIgnoreCase(value)) {
            return null;
        }
        switch (jsonObject.get("type").getAsString().toUpperCase()) {
            case "TIMESTAMP":
                return Timestamp.valueOf(value);
            case "VARCHAR":
                return value;
            case "INTEGER":
                return Integer.parseInt(value);
            case "BIGINT":
                return Long.parseLong(value);
            case "FLOAT":
                return Float.parseFloat(value);
            case "DOUBLE":
                return Double.parseDouble(value);
            case "BOOLEAN":
                return Boolean.parseBoolean(value);
            case "NULL":
                return null;
            default:
                throw new RuntimeException("type = " + jsonObject.get("type").toString().toUpperCase());
        }
    }

    public JsonArray getData() {
        return data;
    }

    public void setData(JsonArray data) {
        this.data = data;
    }
}
