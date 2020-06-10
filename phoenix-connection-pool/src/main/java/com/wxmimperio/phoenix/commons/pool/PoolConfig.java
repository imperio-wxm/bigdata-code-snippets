package com.wxmimperio.phoenix.commons.pool;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PoolConfig {

    /**
     * * [optional]
     * * Pool name. Use unique names if you need multiple pools.
     */
    public String name = "HSQLPool";

    /**
     * * [required]
     * * JDBC connection string
     */
    public String connectionString = null;
    /**
     * * [optional]
     * * Per connection configuration
     */
    public Properties props = null;

    /**
     * * [optional]
     * * There's no upper limit of the connections you can get from the pool. But the pool won't hold all
     * * the connections forever, so this is the number of connections that the pool will keep alive. When a
     * * connection is returned and current pool size is greater than this, then this connection will be closed.
     */
    public int maxConnectionsToKeepAlive;

    /**
     * * [optional]
     * * ClassLoader to load all classes of Phoenix JDBC driver.
     * * Leave it null if you don't care about it.
     */
    public ClassLoader classLoader = null;

    public String PASSWORD = "password";
    public String USERNAME = "username";
    public String DATE_FORMAT_TIME_ZONE = "phoenix.query.dateFormatTimeZone";

    public PoolConfig() {
        String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
        this.maxConnectionsToKeepAlive = 64;
        Properties props = new Properties();
        InputStream input = null;
        try {
            props.load(input);
            Class.forName(driver);
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }
        this.connectionString = props.getProperty("phoenix.query.url");
        props.setProperty(USERNAME, "");
        props.setProperty(PASSWORD, "");
        props.setProperty(DATE_FORMAT_TIME_ZONE, "GMT+8:00");
        this.props = props;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("name = [");
        str.append(name);
        str.append("], connString = [");
        str.append(connectionString);
        str.append("], maxConnectionsToKeepAlive = ");
        str.append(maxConnectionsToKeepAlive);
        str.append(", props = [");
        str.append(props);
        str.append("], classLoader = [");
        str.append(classLoader);
        str.append("]");
        return str.toString();
    }


    private void validateConfig() {
        if (connectionString == null || connectionString.isEmpty()) {
            throw new IllegalArgumentException(
                    "Invalid PhoenixConnectionPool config: connectionString must not be null or empty");
        }
        if (maxConnectionsToKeepAlive < 1) {
            throw new IllegalArgumentException(
                    "Invalid PhoenixConnectionPool config: maxConnectionsToKeepAlive must >= 1");
        }
    }
}
