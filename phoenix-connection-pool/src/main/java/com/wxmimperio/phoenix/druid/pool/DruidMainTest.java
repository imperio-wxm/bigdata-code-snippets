package com.wxmimperio.phoenix.druid.pool;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class DruidMainTest {

    private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);

    public static void main(String[] args) {
        DruidDataSource dataSource = DataSource.getInstance().getDataSource();

        String sql = "select * from PHOENIX_APOLLO.SWY_CHARACTER_LOGIN_GLOG limit 1";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement pst = connection.prepareStatement(sql);
             ResultSet rs = pst.executeQuery()) {
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            while (rs.next()) {
                JSONObject jsonObject = new JSONObject();
                for (int i = 1; i < resultSetMetaData.getColumnCount(); i++) {
                    String colName = resultSetMetaData.getColumnName(i);
                    jsonObject.put(colName, rs.getString(colName));
                }
                LOG.info(jsonObject.toJSONString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DataSource.getInstance().closeDataSource();
        }
    }
}
