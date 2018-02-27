package com.wxmimperio.phoenix.connect;

import com.wxmimperio.phoenix.utils.PropertiesUtils;
import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;
import java.sql.SQLException;

public class ConnectManager {

    private BasicDataSource ds = null;

    private static class ConnectManagerHolder {
        public static ConnectManager instance = new ConnectManager();
    }

    private ConnectManager() {
        initDataSource();
    }

    public static ConnectManager newInstance() {
        return ConnectManagerHolder.instance;
    }

    private void initDataSource() {
        PropertiesUtils properties = new PropertiesUtils("application");
        ds = new BasicDataSource();
        ds.setDriverClassName(properties.getString("phoenix.jdbc.connect.DriverClass"));
        ds.setUrl(properties.getString("phoenix.jdbc.connect.url"));
        ds.setUsername(properties.getString("phoenix.jdbc.connect.username"));
        ds.setPassword(properties.getString("phoenix.jdbc.connect.password"));
    }

    public void shutdownDataSource(DataSource ds) throws SQLException {
        BasicDataSource bds = (BasicDataSource) ds;
        bds.close();
    }

    public BasicDataSource getDs() {
        return ds;
    }
}
