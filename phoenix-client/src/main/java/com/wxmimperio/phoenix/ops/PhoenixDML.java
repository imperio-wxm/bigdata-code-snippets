package com.wxmimperio.phoenix.ops;

import com.alibaba.fastjson.JSONObject;
import com.wxmimperio.phoenix.connect.PhoenixConnectionPool;
import com.wxmimperio.phoenix.utils.PropertiesUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
                    pst.setString(1, jsonData.get("id").toString());
                    pst.setString(2, jsonData.get("name").toString());
                    pst.setString(3, jsonData.get("age").toString());
                    pst.setString(4, jsonData.get("gender").toString());
                    pst.addBatch();
                }
                pst.executeBatch();
                connection.commit();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Connection getPhoenixConnection() throws SQLException {
        return phoenixConnectionPool.getConnection();
    }
}
