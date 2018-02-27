package com.wxmimperio.hbase.hbaseconnect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HBaseConnect {
    private static Logger LOG = LoggerFactory.getLogger(HBaseConnect.class);

    private Connection connection;
    private static String HBASE_SITE = "hbaes-site.xml";

    public HBaseConnect(String quorum, String port) throws IOException {
        Configuration conf = initConfig();
        conf.set("hbase.zookeeper.quorum", quorum);
        conf.set("hbase.zookeeper.property.clientPort", port);
        this.connection = getConnection(conf);
    }

    public HBaseConnect() throws IOException {
        this.connection = getConnection(initConfig());
    }

    private Configuration initConfig() {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(HBASE_SITE);
        return conf;
    }

    private Connection getConnection(Configuration configuration) throws IOException {
        String poolSize = configuration.get("hbase.connect.pool.size");
        ExecutorService pool = null;
        if (poolSize != null && !poolSize.isEmpty()) {
            pool = Executors.newFixedThreadPool(Integer.parseInt(poolSize));
        }
        return ConnectionFactory.createConnection(configuration, pool);
    }

    public void close() throws IOException {
        if (connection != null) {
            connection.close();
        }
        LOG.info("Get hbase connect closed!");
    }

    public Connection getConnection() {
        return connection;
    }
}
