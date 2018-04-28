package com.wxmimperio.phoenix;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class PhoenixPool {

    private static final Logger LOG = LoggerFactory.getLogger(PhoenixPool.class);
    private GenericObjectPool<Connection> pool;
    private static final int ZOOKEEPER_CONN_POOL_COUNT = 1;

    private static class SingletonHolder {
        private static final PhoenixPool INSTANCE = new PhoenixPool();
    }

    public static PhoenixPool getInstance() {
        return SingletonHolder.INSTANCE;
    }

    private PhoenixPool() {
        GenericObjectPoolConfig conf = new GenericObjectPoolConfig();
        conf.setMaxTotal(ZOOKEEPER_CONN_POOL_COUNT);
        PoolConfig poolConfig = new PoolConfig();
        PhoenixPoolFactory phoenixPoolFactory = new PhoenixPoolFactory(poolConfig);
        pool = new GenericObjectPool<>(phoenixPoolFactory, conf);
        phoenixPoolFactory.setPool(pool);
    }

    public Connection getConnection() throws Exception {
        return pool.borrowObject();
    }

    public void returnConnection(Connection connection) {
        pool.returnObject(connection);
    }

}
