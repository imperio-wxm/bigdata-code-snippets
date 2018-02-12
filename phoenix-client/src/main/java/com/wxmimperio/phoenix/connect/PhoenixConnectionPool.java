package com.wxmimperio.phoenix.connect;

import org.apache.phoenix.jdbc.PhoenixDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

public class PhoenixConnectionPool {
    public static class PoolConfig {
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
        public int maxConnectionsToKeepAlive = 64;

        /**
         * * [optional]
         * * ClassLoader to load all classes of Phoenix JDBC driver.
         * * Leave it null if you don't care about it.
         */
        public ClassLoader classLoader = null;

        public PoolConfig(String connectionString) {
            this.connectionString = connectionString;
        }

        public PoolConfig(String connectionString, Properties props) {
            this.connectionString = connectionString;
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

    private static Logger LOG = LoggerFactory.getLogger(PhoenixConnectionPool.class);

    private PoolConfig cfg;
    private ConcurrentLinkedQueue<Connection> pool = new ConcurrentLinkedQueue<Connection>();

    public PhoenixConnectionPool(String connectionString) {
        this(new PoolConfig(connectionString));
    }

    public PhoenixConnectionPool(String connectionString, Properties props) {
        this(new PoolConfig(connectionString, props));
    }

    public PhoenixConnectionPool(PoolConfig cfg) {
        // validate config
        cfg.validateConfig();
        this.cfg = cfg;

        if (!this.cfg.connectionString.contains("thin")) {
            // load JDBC driver class
            try {
                if (cfg.classLoader != null) {
                    Class.forName(PhoenixDriver.class.getName(), true, cfg.classLoader);
                } else {
                    Class.forName(PhoenixDriver.class.getName());
                }
            } catch (Throwable t) {
                LOG.error("Failed loading Phoenix JDBC driver", t);
                throw new RuntimeException("Failed loading Phoenix JDBC driver", t);
            }
        }
        // dump the configurations to log
        LOG.info("PhoenixConnectionPool created. config [" + cfg.toString() + "]");
    }

    /**
     * Close all connections currently in this pool.
     * Before calling this method, please make sure that all connections retrieved from this pool have been closed.
     */
    public void close() throws SQLException {
        while (true) {
            PDelegateConnection conn = (PDelegateConnection) pool.poll();
            if (conn != null) {
                conn.getDelegate().close();
            } else {
                return;
            }
        }
    }

    /**
     * Get a connection.
     * Client should call {@link Connection#close()} after use and the connection will be automatically returned to pool.
     */
    public Connection getConnection() throws SQLException {
        Connection conn = pool.poll();
        if (conn != null) {
            return conn;
        } else {
            if (cfg.props != null) {
                return new PDelegateConnection(DriverManager.getConnection(cfg.connectionString, cfg.props));
            } else {
                return new PDelegateConnection(DriverManager.getConnection(cfg.connectionString));
            }
        }
    }

    private void putConnection(Connection conn) throws SQLException {
        PDelegateConnection dc = (PDelegateConnection) conn;
        if (pool.size() > cfg.maxConnectionsToKeepAlive) {
            // there's enough connection in pool, close this one
            dc.getDelegate().close();
        } else {
            // put this connection to pool
            pool.add(conn);
        }
    }


    private class PDelegateConnection implements Connection {

        private Connection delegate;

        public PDelegateConnection(Connection delegate) {
            this.delegate = delegate;
        }


        public Connection getDelegate() {
            return this.delegate;
        }


        @Override
        public void close() throws SQLException {
            // don't actually close this connection, just return it back to pool
            putConnection(this);
        }


        @Override
        public Statement createStatement() throws SQLException {
            return delegate.createStatement();

        }


        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            return delegate.prepareStatement(sql);

        }


        @Override
        public CallableStatement prepareCall(String sql) throws SQLException {
            return delegate.prepareCall(sql);

        }


        @Override
        public String nativeSQL(String sql) throws SQLException {
            return delegate.nativeSQL(sql);

        }


        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
            delegate.setAutoCommit(autoCommit);

        }


        @Override
        public boolean getAutoCommit() throws SQLException {
            return delegate.getAutoCommit();

        }


        @Override
        public void commit() throws SQLException {
            delegate.commit();

        }


        @Override
        public void rollback() throws SQLException {
            delegate.rollback();

        }


        @Override
        public boolean isClosed() throws SQLException {
            return delegate.isClosed();

        }


        @Override
        public DatabaseMetaData getMetaData() throws SQLException {
            return delegate.getMetaData();

        }


        @Override
        public void setReadOnly(boolean readOnly) throws SQLException {
            delegate.setReadOnly(readOnly);

        }


        @Override
        public boolean isReadOnly() throws SQLException {
            return delegate.isReadOnly();

        }


        @Override
        public void setCatalog(String catalog) throws SQLException {
            delegate.setCatalog(catalog);

        }


        @Override
        public String getCatalog() throws SQLException {
            return delegate.getCatalog();

        }


        @Override
        public void setTransactionIsolation(int level) throws SQLException {
            delegate.setTransactionIsolation(level);

        }


        @Override
        public int getTransactionIsolation() throws SQLException {
            return delegate.getTransactionIsolation();

        }


        @Override
        public SQLWarning getWarnings() throws SQLException {
            return delegate.getWarnings();

        }


        @Override
        public void clearWarnings() throws SQLException {
            delegate.clearWarnings();

        }


        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            return delegate.createStatement(resultSetType, resultSetConcurrency);

        }


        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency);

        }


        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return delegate.prepareCall(sql, resultSetType, resultSetConcurrency);

        }


        @Override
        public java.util.Map<String, Class<?>> getTypeMap() throws SQLException {
            return delegate.getTypeMap();

        }


        @Override
        public void setTypeMap(java.util.Map<String, Class<?>> map) throws SQLException {
            delegate.setTypeMap(map);

        }


        @Override
        public void setHoldability(int holdability) throws SQLException {
            delegate.setHoldability(holdability);

        }


        @Override
        public int getHoldability() throws SQLException {
            return delegate.getHoldability();

        }


        @Override
        public Savepoint setSavepoint() throws SQLException {
            return delegate.setSavepoint();

        }


        @Override
        public Savepoint setSavepoint(String name) throws SQLException {
            return delegate.setSavepoint(name);

        }


        @Override
        public void rollback(Savepoint savepoint) throws SQLException {
            delegate.rollback(savepoint);

        }


        @Override
        public void releaseSavepoint(Savepoint savepoint) throws SQLException {
            delegate.releaseSavepoint(savepoint);

        }


        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);

        }


        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                                  int resultSetHoldability) throws SQLException {
            return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);

        }


        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                             int resultSetHoldability) throws SQLException {
            return delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);

        }

        @Override
        public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
            return delegate.prepareStatement(sql, autoGeneratedKeys);

        }


        @Override
        public PreparedStatement prepareStatement(String sql, int columnIndexes[]) throws SQLException {
            return delegate.prepareStatement(sql, columnIndexes);

        }


        @Override
        public PreparedStatement prepareStatement(String sql, String columnNames[]) throws SQLException {
            return delegate.prepareStatement(sql, columnNames);

        }


        @Override
        public Clob createClob() throws SQLException {
            return delegate.createClob();

        }


        @Override
        public Blob createBlob() throws SQLException {
            return delegate.createBlob();

        }


        @Override
        public NClob createNClob() throws SQLException {
            return delegate.createNClob();

        }


        @Override
        public SQLXML createSQLXML() throws SQLException {
            return delegate.createSQLXML();

        }


        @Override
        public boolean isValid(int timeout) throws SQLException {
            return delegate.isValid(timeout);

        }


        @Override
        public void setClientInfo(String name, String value) throws SQLClientInfoException {
            delegate.setClientInfo(name, value);

        }


        @Override
        public void setClientInfo(Properties properties) throws SQLClientInfoException {
            delegate.setClientInfo(properties);

        }


        @Override
        public String getClientInfo(String name) throws SQLException {
            return delegate.getClientInfo(name);

        }


        @Override
        public Properties getClientInfo() throws SQLException {
            return delegate.getClientInfo();

        }


        @Override
        public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            return delegate.createArrayOf(typeName, elements);

        }


        @Override
        public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            return delegate.createStruct(typeName, attributes);

        }


        @Override
        public void setSchema(String schema) throws SQLException {
            delegate.setSchema(schema);

        }


        @Override
        public String getSchema() throws SQLException {
            return delegate.getSchema();

        }


        @Override
        public void abort(Executor executor) throws SQLException {
            delegate.abort(executor);

        }


        @Override
        public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
            delegate.setNetworkTimeout(executor, milliseconds);

        }


        @Override
        public int getNetworkTimeout() throws SQLException {
            return delegate.getNetworkTimeout();

        }


        @Override
        public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
            return delegate.unwrap(iface);

        }


        @Override
        public boolean isWrapperFor(java.lang.Class<?> iface) throws java.sql.SQLException {
            return delegate.isWrapperFor(iface);

        }
    }
}
