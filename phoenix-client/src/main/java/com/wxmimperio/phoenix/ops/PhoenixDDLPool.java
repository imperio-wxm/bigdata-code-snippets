package com.wxmimperio.phoenix.ops;

import com.wxmimperio.phoenix.connect.PhoenixConnectionPool;
import com.wxmimperio.phoenix.utils.PropertiesUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class PhoenixDDLPool {

    private PhoenixConnectionPool pool = null;
    private PropertiesUtils properties = null;

    public PhoenixDDLPool() {
        this.properties = new PropertiesUtils("application");
        this.pool = new PhoenixConnectionPool(properties.getPhoenixConnectUrl());
    }

    public void executeSql(String sql) {
        Connection conn = null;
        Statement stmt = null;

        try {
            conn = this.pool.getConnection();
            stmt = conn.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
