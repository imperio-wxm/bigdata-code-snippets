package com.wxmimperio.phoenix;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.*;

public class MainPoolTest {
    private static final Logger LOG = LoggerFactory.getLogger(MainPoolTest.class);
    private static ExecutorService executorService = Executors.newFixedThreadPool(50);

    public static void main(String[] args) throws Exception {

        PhoenixPool phoenixPool = PhoenixPool.getInstance();
        LOG.info("getNumActive = " + phoenixPool.getNumActive());
        LOG.info("getNumIdle = " + phoenixPool.getNumIdle());

        int index = 0;
        while (index < 10000000) {
            List<Future> futureList = Lists.newArrayList();
            for (int i = 1; i <= 30; i++) {
                LOG.info("run getNumActive = " + phoenixPool.getNumActive());
                LOG.info("run getNumIdle = " + phoenixPool.getNumIdle());
                if (i % 10 == 0) {
                    LOG.info("Sleep....");
                    Thread.sleep(60 * 1000 * 5);
                }
                futureList.add(executorService.submit(new Task(phoenixPool)));
            }

            futureList.stream().forEach(future -> {
                try {
                    LOG.info("done getNumActive = " + phoenixPool.getNumActive());
                    LOG.info("done getNumIdle = " + phoenixPool.getNumIdle());
                    LOG.info("Result = " + future.get());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            });
            index++;
            LOG.info("index ====== " + index + " finish!!!");
        }
        phoenixPool.closePool();
        LOG.info("close getNumActive = " + phoenixPool.getNumActive());
        LOG.info("close getNumIdle = " + phoenixPool.getNumIdle());
        executorService.shutdown();
    }

    static class Task implements Callable<Boolean> {
        PhoenixPool phoenixPool;

        public Task(PhoenixPool phoenixPool) {
            this.phoenixPool = phoenixPool;
        }

        @Override
        public Boolean call() {
            String sql = "select * from PHOENIX_APOLLO.SWY_CHARACTER_LOGIN_GLOG limit 1";
            try (Connection connection = phoenixPool.getConnection();
                 Statement pst = connection.createStatement()) {
                LOG.info("select getNumActive = " + phoenixPool.getNumActive());
                LOG.info("select getNumIdle = " + phoenixPool.getNumIdle());
                ResultSet resultSet = pst.executeQuery(sql);
                ResultSetMetaData meta = resultSet.getMetaData();
                while (resultSet.next()) {
                    JSONObject jsonObject = new JSONObject();
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        String colName = meta.getColumnName(i);
                        jsonObject.put(colName, String.valueOf(resultSet.getObject(colName)));
                    }
                    LOG.info(Thread.currentThread().getName() + " running....");
                    LOG.info(jsonObject.toJSONString());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return true;
        }
    }
}
