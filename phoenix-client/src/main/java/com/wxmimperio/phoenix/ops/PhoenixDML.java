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
                    int index = 1;
                    for (Map.Entry<String, Object> entry : jsonData.entrySet()) {
                        pst.setString(index, entry.getValue().toString());
                        index++;
                    }
                    pst.addBatch();
                }
                pst.executeBatch();
                connection.commit();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {


    }

    private Connection getPhoenixConnection() throws SQLException {
        return phoenixConnectionPool.getConnection();
    }
}
