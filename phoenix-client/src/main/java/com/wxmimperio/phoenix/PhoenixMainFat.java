package com.wxmimperio.phoenix;

import com.alibaba.fastjson.JSONObject;
import org.apache.phoenix.shaded.org.apache.commons.configuration.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class PhoenixMainFat {

    public static void main(String[] args) {
        String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
        String url = "";

        InputStream input = Configuration.class.getResourceAsStream("/phoenix-site.properties");
        Properties props = new Properties();
        try {
            props.load(input);
            Class.forName(driver);
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }

        String selectSql = "";

        try (Connection connection = DriverManager.getConnection(url, props);
             Statement pst = connection.createStatement()) {
            ResultSet resultSet = pst.executeQuery(selectSql);
            ResultSetMetaData meta = resultSet.getMetaData();
            while (resultSet.next()) {
                JSONObject jsonObject = new JSONObject();
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    String colName = meta.getColumnName(i);
                    jsonObject.put(colName, String.valueOf(resultSet.getObject(colName)));
                }
                System.out.println(jsonObject);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
