package com.wxmimperio.hbase;

import com.google.common.cache.*;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class HBaseClientManager {
    private static Logger LOG = LoggerFactory.getLogger(HBaseClientManager.class);

    private Cache<String, HBaseClient> clientCache;
    private ExecutorService cacheRemovePool;
    private Connection connection;

    public HBaseClientManager(Connection connection) {
        this.connection = connection;
        this.cacheRemovePool = Executors.newFixedThreadPool(10);
        this.clientCache = CacheBuilder.newBuilder()
                .maximumSize(100000)
                .expireAfterAccess(30, TimeUnit.MINUTES)
                .removalListener(RemovalListeners.asynchronous(new RemoveFileListener(), cacheRemovePool))
                .recordStats()
                .build();
    }

    private class RemoveFileListener implements RemovalListener<String, HBaseClient> {
        public void onRemoval(RemovalNotification<String, HBaseClient> notification) {
            try {
                // delete cache
                notification.getValue();
                Thread.sleep(5000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            LOG.info("TableName = " + notification.getKey() + ", HBaseClient removed. Remove cause = " + notification.getCause());
        }
    }

    private HBaseClient get(String key) throws ExecutionException {
        HBaseClient hBaseClient = clientCache.get(key, new Callable<HBaseClient>() {
            @Override
            public HBaseClient call() throws Exception {
                return new HBaseClient(connection, "");
            }
        });
        return hBaseClient;
    }

    private void put(String key, HBaseClient hBaseClient) {
        clientCache.put(key, hBaseClient);
    }

    public HBaseClient getHBaseClient(String tableName) throws Exception {
        HBaseClient hBaseClient;
        if (!clientCache.asMap().containsKey(tableName)) {
            hBaseClient = new HBaseClient(connection, tableName);
            put(tableName, hBaseClient);
        } else {
            hBaseClient = get(tableName);
        }
        return hBaseClient;
    }

    public void dispose() {
        clientCache.invalidateAll();
        LOG.info("Finish clean cache...");
        cacheRemovePool.shutdown();
        try {
            while (!cacheRemovePool.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.info("Wait cache pool shutdown.....");
            }
        } catch (Exception e) {
            LOG.error("Shutdown cache pool error.", e);
        }
    }
}
