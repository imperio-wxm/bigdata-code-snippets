package com.wxmimperio.hadoop.Deduplication.connection;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class HiveConnectionPool {
    private static final Logger LOG = Logger.getLogger(HiveConnectionPool.class);

    private HikariDataSource hikariDataSource;

    private HiveConnectionPool() {
        initPool();
    }

    private static class SingletonInstance {
        private static final HiveConnectionPool INSTANCE = new HiveConnectionPool();
    }

    public static HiveConnectionPool getInstance() {
        return SingletonInstance.INSTANCE;
    }

    public HikariDataSource getHikariDataSource() {
        return hikariDataSource;
    }

    private void initPool() {
        Properties props = new Properties();
        try (InputStream is = this.getClass().getResourceAsStream("/config.properties")) {
            props.load(is);
            LOG.info("add properties = " + props);
        } catch (IOException e) {
            LOG.error("Can not get properties.", e);
        }
        this.hikariDataSource = new HikariDataSource();
        this.hikariDataSource.setDriverClassName(props.getProperty("hive.jdbc.driverClassName"));
        this.hikariDataSource.setJdbcUrl(props.getProperty("hive.jdbc.connection.url"));
        this.hikariDataSource.setUsername(props.getProperty("hive.jdbc.connection.username"));
        this.hikariDataSource.setPassword(props.getProperty("hive.jdbc.connection.password"));
        this.hikariDataSource.setIdleTimeout(Long.parseLong(props.getProperty("hive.jdbc.idleTimeout")));
        this.hikariDataSource.setMaxLifetime(Long.parseLong(props.getProperty("hive.jdbc.maxLifetime")));
        this.hikariDataSource.setConnectionTestQuery(props.getProperty("hive.jdbc.connectionTestQuery"));
        this.hikariDataSource.setMinimumIdle(Integer.parseInt(props.getProperty("hive.jdbc.minimumIdle")));
        this.hikariDataSource.setMaximumPoolSize(Integer.parseInt(props.getProperty("hive.jdbc.maxConn")));
        this.hikariDataSource.setValidationTimeout(Integer.parseInt(props.getProperty("hive.jdbc.validationTimeout")));
        this.hikariDataSource.setLeakDetectionThreshold(Long.parseLong(props.getProperty("hive.jdbc.leakDetectionThreshold")));
        this.hikariDataSource.setPoolName(props.getProperty("hive.jdbc.poolName"));
    }

    public void closePool() {
        this.hikariDataSource.close();
    }
}
