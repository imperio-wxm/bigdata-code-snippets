package com.wxmimperio.phoenix.ops;

import com.wxmimperio.phoenix.connect.ConnectManager;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class PhoenixDDL {
    private BasicDataSource dataSource;

    public PhoenixDDL() {
        this.dataSource = ConnectManager.newInstance().getDs();
    }

    public void createTable(String sql) {
        Connection conn = null;
        Statement stmt = null;

        try {
            conn = dataSource.getConnection();
            stmt = conn.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(conn, stmt);
        }
    }

    private void close(Connection conn, Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void closeDataSource() throws SQLException {
        ConnectManager.newInstance().shutdownDataSource(this.dataSource);
    }
}
