package com.wxmimperio.phoenix.druid.pool;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.concurrent.*;

public class DruidMainTest {

    private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);
    private static ExecutorService executorService = Executors.newFixedThreadPool(50);

    public static void main(String[] args) throws Exception {
        DruidDataSource dataSource = DataSource.getInstance().getDataSource();

        int index = 0;
        while (index < 10000000) {
            List<Future> futureList = Lists.newArrayList();
            for (int i = 1; i <= 30; i++) {
                futureList.add(executorService.submit(new Task(dataSource)));
            }

            futureList.stream().forEach(future -> {
                try {
                    LOG.info(Thread.currentThread().getName() + " run getActiveCount = " + dataSource.getActiveCount());
                    LOG.info(Thread.currentThread().getName() + " run getDiscardCount = " + dataSource.getDiscardCount());
                    LOG.info(Thread.currentThread().getName() + " run getCreateCount = " + dataSource.getCreateCount());
                    LOG.info(Thread.currentThread().getName() + " run getDestroyCount = " + dataSource.getDestroyCount());
                    LOG.info(Thread.currentThread().getName() + " run getRecycleCount = " + dataSource.getRecycleCount());
                    LOG.info("Result = " + future.get());
                    LOG.info("===========================================");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            });
            index++;
            Thread.sleep(5 * 60 * 1000);
            LOG.info("sleep..........");
            LOG.info("index ====== " + index + " finish!!!");
        }
        DataSource.getInstance().closeDataSource();
        executorService.shutdown();
    }


    static class Task implements Callable<Boolean> {
        DruidDataSource dataSource;

        public Task(DruidDataSource dataSource) {
            this.dataSource = dataSource;
        }

        @Override
        public Boolean call() {
            String sql = "select * from PHOENIX_APOLLO.SWY_CHARACTER_LOGIN_GLOG limit 1";

            try (Connection connection = dataSource.getConnection();
                 PreparedStatement pst = connection.prepareStatement(sql);
                 ResultSet rs = pst.executeQuery()) {
                ResultSetMetaData resultSetMetaData = rs.getMetaData();
                while (rs.next()) {
                    JSONObject jsonObject = new JSONObject();
                    for (int i = 1; i < resultSetMetaData.getColumnCount(); i++) {
                        String colName = resultSetMetaData.getColumnName(i);
                        jsonObject.put(colName, rs.getString(colName));
                    }
                    LOG.info(jsonObject.toJSONString());
                }
                LOG.info(Thread.currentThread().getName() + " select getActiveCount = " + dataSource.getActiveCount());
                LOG.info(Thread.currentThread().getName() + " select getDiscardCount = " + dataSource.getDiscardCount());
                LOG.info(Thread.currentThread().getName() + " select getCreateCount = " + dataSource.getCreateCount());
                LOG.info(Thread.currentThread().getName() + " select getDestroyCount = " + dataSource.getDestroyCount());
                LOG.info(Thread.currentThread().getName() + " select getRecycleCount = " + dataSource.getRecycleCount());
            } catch (Exception e) {
                e.printStackTrace();
            }
            return true;
        }
    }
}
