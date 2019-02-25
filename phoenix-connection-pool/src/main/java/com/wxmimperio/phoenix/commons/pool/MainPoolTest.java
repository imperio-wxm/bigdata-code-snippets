package com.wxmimperio.phoenix.commons.pool;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
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
        boolean isFirstTime = true;
        while (index < 10000000) {
            List<Future> futureList = new ArrayList<>();
            for (int i = 1; i <= 30; i++) {
                LOG.info("run getNumActive = " + phoenixPool.getNumActive());
                LOG.info("run getNumIdle = " + phoenixPool.getNumIdle());
                if (i % 10 == 0 && !isFirstTime) {
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
            // 保证第一次30个全部执行，之后再进行sleep
            isFirstTime = false;
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
                ResultSet resultSet = pst.executeQuery(sql);
                ResultSetMetaData meta = resultSet.getMetaData();
                while (resultSet.next()) {
                    JSONObject jsonObject = new JSONObject();
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        String colName = meta.getColumnName(i);
                        jsonObject.put(colName, String.valueOf(resultSet.getObject(colName)));
                    }
                    LOG.info(jsonObject.toJSONString());
                }
                LOG.info(Thread.currentThread().getName() + "select getNumActive = " + phoenixPool.getNumActive());
                LOG.info(Thread.currentThread().getName() + "select getNumIdle = " + phoenixPool.getNumIdle());
            } catch (Exception e) {
                e.printStackTrace();
            }
            return true;
        }
    }
}
