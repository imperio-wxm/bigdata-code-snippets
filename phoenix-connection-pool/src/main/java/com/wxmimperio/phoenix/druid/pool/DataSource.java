package com.wxmimperio.phoenix.druid.pool;

import com.alibaba.druid.pool.DruidDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSource {
    private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);

    private DruidDataSource dataSource;

    private static class SingletonHolder {
        private static final DataSource INSTANCE = new DataSource();
    }

    public static DataSource getInstance() {
        return SingletonHolder.INSTANCE;
    }

    public DataSource() {
        this.dataSource = new DruidDataSource();
        initDataSource();
    }

    private void initDataSource() {
        dataSource.setDriverClassName("org.apache.phoenix.jdbc.PhoenixDriver");
        dataSource.setUrl("");
        // 初始化时建立物理连接的个数。初始化发生在显示调用init方法，或者第一次getConnection时
        dataSource.setInitialSize(1);
        // 最大连接池数量
        dataSource.setMaxActive(2);
        // 最小连接池数量
        dataSource.setMinIdle(1);
        // 用来检测连接是否有效的sql，要求是一个查询语句。如果validationQuery为null，testOnBorrow、testOnReturn、testWhileIdle都不会其作用。
        dataSource.setValidationQuery("SELECT 1");
        // 建议配置为true，不影响性能，并且保证安全性。申请连接的时候检测，如果空闲时间大于timeBetweenEvictionRunsMillis，执行validationQuery检测连接是否有效。
        dataSource.setTestWhileIdle(true);
        // 有两个含义 1) Destroy线程会检测连接的间隔时间 2) testWhileIdle的判断依据，详细看testWhileIdle属性的说明
        dataSource.setTimeBetweenEvictionRunsMillis(10 * 1000);
        dataSource.setTestOnBorrow(false);
        dataSource.setTestOnReturn(false);
        /*
        初始化连接池时会填充到minIdle数量。
        连接池中的minIdle数量以内的连接，空闲时间超过minEvictableIdleTimeMillis，则会执行keepAlive操作。
        当网络断开等原因产生的由ExceptionSorter检测出来的死连接被清除后，自动补充连接到minIdle数量。
        */
        dataSource.setKeepAlive(true);
        dataSource.setMinEvictableIdleTimeMillis(30 * 1000);
    }

    public DruidDataSource getDataSource() {
        return dataSource;
    }

    public void closeDataSource() {
        dataSource.close();
        LOG.info("Data source closed!");
    }
}
