package com.wxmimperio.phoenix.ops;

import com.alibaba.fastjson.JSONObject;
import com.wxmimperio.phoenix.connect.PhoenixConnectionPool;
import com.wxmimperio.phoenix.utils.PropertiesUtils;

import java.sql.*;
import java.util.List;
import java.util.Map;

public class PhoenixDML {

    private PhoenixConnectionPool phoenixConnectionPool;
    private PropertiesUtils properties = null;

    public PhoenixDML() {
        this.properties = new PropertiesUtils("application");
        this.phoenixConnectionPool = new PhoenixConnectionPool(properties.getPhoenixConnectUrl());
    }

    public void insert(String sql, List<JSONObject> jsonObjects) {
        try {
            try (Connection connection = getPhoenixConnection();
                 PreparedStatement pst = connection.prepareStatement(sql)) {
                connection.setAutoCommit(false);
                for (JSONObject jsonData : jsonObjects) {
                    pst.setString(1, jsonData.get("MESSAGE_KEY").toString());
                    pst.setString(2, jsonData.get("EVENT_TIME").toString());
                    pst.setString(3, jsonData.get("GAME_ID").toString());
                    pst.setLong(4, Long.parseLong(jsonData.get("AREA_ID").toString()));
                    pst.setLong(5, Long.parseLong(jsonData.get("GROUP_ID").toString()));
                    pst.setString(6, jsonData.get("PLATFORM").toString());
                    pst.setString(7, jsonData.get("CHANNEL_ID").toString());
                    pst.setString(8, jsonData.get("MID").toString());
                    pst.setLong(9, Long.parseLong(jsonData.get("CHARACTER_ID").toString()));
                    pst.setLong(10, Long.parseLong(jsonData.get("CHARACTER_LEVEL").toString()));
                    pst.setLong(11, Long.parseLong(jsonData.get("TASK_ID").toString()));
                    pst.setLong(12, Long.parseLong(jsonData.get("TASK_TYPE").toString()));
                    pst.setLong(13, Long.parseLong(jsonData.get("OPT_TYPE").toString()));
                    pst.setLong(14, Long.parseLong(jsonData.get("SERIAL_NUM").toString()));
                    pst.addBatch();
                    System.out.println(jsonData);
                }
                pst.executeBatch();
                connection.commit();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void select(String sql) {
        try {
            try (Connection connection = getPhoenixConnection();
                 Statement pst = connection.createStatement()) {
                ResultSet resultSet = pst.executeQuery(sql);
                ResultSetMetaData meta = resultSet.getMetaData();
                while (resultSet.next()) {
                    JSONObject jsonObject = new JSONObject();
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        String colName = meta.getColumnName(i);
                        jsonObject.put(colName, String.valueOf(resultSet.getObject(colName)));
                    }
                    System.out.println(jsonObject);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Connection getPhoenixConnection() throws SQLException {
        return phoenixConnectionPool.getConnection();
    }
}
