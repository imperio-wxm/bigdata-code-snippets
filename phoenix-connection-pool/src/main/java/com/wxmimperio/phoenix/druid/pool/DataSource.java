package com.wxmimperio.phoenix.druid.pool;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class DataSource {
    private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);

    public static void main(String[] args) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName("org.apache.phoenix.jdbc.PhoenixDriver");
        dataSource.setUrl("");
        dataSource.setUsername("");
        dataSource.setPassword("");
        try {
            conn = dataSource.getConnection();
            String sql = "";
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            while (rs.next()) {
                JSONObject jsonObject = new JSONObject();
                for (int i = 1; i < resultSetMetaData.getColumnCount(); i++) {
                    String columnName = resultSetMetaData.getColumnName(i);
                    jsonObject.put(columnName, rs.getString(columnName));
                }
                LOG.info(jsonObject.toJSONString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                pstmt.close();
                rs.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
