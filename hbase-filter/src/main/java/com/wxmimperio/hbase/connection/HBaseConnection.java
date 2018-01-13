package com.wxmimperio.hbase.connection;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HBaseConnection {
    private static Logger LOG = LoggerFactory.getLogger(HBaseConnection.class);

    public static Connection connection;
    private static String HBASE_SITE = "hbaes-site.xml";


    static {
        initHbase();
    }

    private static void initHbase() {
        connection = getConnection();
    }

    private synchronized static Connection getConnection() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(HBASE_SITE);
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            LOG.error("Get hbase connect error !", e);
        }
        return connection;
    }
}
