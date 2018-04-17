package com.wxmimperio.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class HBaseConnection implements Serializable {

    private static Logger LOG = LoggerFactory.getLogger(HBaseClientOps.class);

    private static String HBASE_SITE = "hbaes-site.xml";
    private static Connection connection;

    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(HBASE_SITE);
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        if (connection == null) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        LOG.info("HBase client build success!");
    }

    public Connection getConnection() {
        return connection;
    }
}
