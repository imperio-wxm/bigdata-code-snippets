package com.wxmimperio.presto.controller;

import com.google.common.collect.Lists;
import com.wxmimperio.presto.service.QueryService;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.*;

@RestController("presto")
public class QueryController {

    private static final Logger LOG = Logger.getLogger(QueryController.class);
    private QueryService queryService;
    private ExecutorService executors;

    @Autowired
    public QueryController(QueryService queryService) {
        this.queryService = queryService;
        this.executors = Executors.newFixedThreadPool(100);
    }

    @PostMapping("query")
    public Integer query(@RequestParam String sql) {
        int resultSize = 0;
        LOG.info("=================Query Start====================");
        long start = System.currentTimeMillis();
        try {
            if (sql.contains(";")) {
                for (String subSql : sql.split(";", -1)) {
                    String runSubSql = subSql.trim();
                    if (!StringUtils.isEmpty(runSubSql)) {
                        resultSize += queryService.queryGetResult(runSubSql).size();
                    }
                }
            } else {
                resultSize = queryService.queryGetResult(sql).size();
            }
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Can not exe sql = %s", sql), e);
        }
        LOG.info(String.format("=====This query total cost %s ms, all size = %s", (System.currentTimeMillis() - start), resultSize));
        LOG.info("==================Query End==================");
        return resultSize;
    }

    @PostMapping("concurrentQuery")
    public Integer concurrentQuery(@RequestParam String sql) {
        return getResult(sql);
    }

    private int getResult(String sql) {
        int resultSize = 0;
        LOG.info("=================Query Start====================");
        long start = System.currentTimeMillis();
        try {
            if (sql.contains(";")) {
                List<Future<Integer>> futures = Lists.newArrayList();
                for (String subSql : sql.split(";", -1)) {
                    String runSubSql = subSql.trim();
                    if (!StringUtils.isEmpty(runSubSql)) {
                        futures.add(executors.submit(() -> queryService.queryGetResult(runSubSql).size()));
                    }
                }
                for (Future<Integer> future : futures) {
                    resultSize += future.get();
                }
            } else {
                resultSize = queryService.queryGetResult(sql).size();
            }
        } catch (InterruptedException | ExecutionException | SQLException e) {
            throw new RuntimeException(String.format("Can not exe sql = %s", sql), e);
        }
        LOG.info(String.format("=====This query total cost %s ms, all size = %s", (System.currentTimeMillis() - start), resultSize));
        LOG.info("==================Query End==================");
        return resultSize;
    }
}
