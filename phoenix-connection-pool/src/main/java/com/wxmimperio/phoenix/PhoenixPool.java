package com.wxmimperio.phoenix;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class PhoenixPool {

    private static final Logger LOG = LoggerFactory.getLogger(PhoenixPool.class);
    private GenericObjectPool<Connection> pool;
    private static final int ZOOKEEPER_CONN_POOL_COUNT = 50;

    private static class SingletonHolder {
        private static final PhoenixPool INSTANCE = new PhoenixPool();
    }

    public static PhoenixPool getInstance() {
        return SingletonHolder.INSTANCE;
    }

    private PhoenixPool() {
        GenericObjectPoolConfig conf = new GenericObjectPoolConfig();
        conf.setMaxTotal(ZOOKEEPER_CONN_POOL_COUNT);
        // 在检测空闲对象线程检测到对象不需要移除时，是否检测对象的有效性。true是，默认值是false。
        conf.setTestWhileIdle(true);
        // 空闲对象检测线程的执行周期，即多长时候执行一次空闲对象检测。单位是毫秒数。如果小于等于0，则不执行检测线程。默认值是-1;
        conf.setTimeBetweenEvictionRunsMillis(60 * 1000);
        // 在向对象池中归还对象时是否检测对象有效，true是，默认值是false。
        conf.setTestOnReturn(true);

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

    /**
     * 获取空闲对象个数
     *
     * @return
     */
    public int getNumIdle() {
        return pool.getNumIdle();
    }

    /**
     * 获取活跃对象个数
     *
     * @return
     */
    public int getNumActive() {
        return pool.getNumActive();
    }

    public void closePool() {
        pool.close();
        LOG.info("Pool closed!");
    }
}
