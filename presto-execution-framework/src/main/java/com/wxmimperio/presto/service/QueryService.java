package com.wxmimperio.presto.service;


import com.facebook.presto.jdbc.PrestoResultSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.wxmimperio.presto.connection.PrestoConnection;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.List;
import java.util.Map;

@Component
public class QueryService {

    private static final Logger LOG = Logger.getLogger(QueryService.class);

    private PrestoConnection prestoConnection;

    @Autowired
    public QueryService(PrestoConnection prestoConnection) {
        this.prestoConnection = prestoConnection;
    }

    public List<Map<String, Object>> queryGetResult(String querySql) throws SQLException {
        List<Map<String, Object>> results = Lists.newArrayList();
        try (Connection connection = getConnection();
             Statement statement = connection.createStatement()) {
            long start = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(querySql);
            String queryId = resultSet.unwrap(PrestoResultSet.class).getQueryId();
            List<String> colNames = Lists.newArrayList();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                colNames.add(resultSetMetaData.getColumnName(i));
            }
            while (resultSet.next()) {
                Map<String, Object> line = Maps.newHashMap();
                for (String colName : colNames) {
                    line.put(colName, resultSet.getObject(colName));
                }
                results.add(line);
            }
            LOG.info(String.format("QueryId = %s, Just query cost = %s ms", queryId, (System.currentTimeMillis() - start)));
        } catch (SQLException e) {
            throw new SQLException(String.format("Can not exe sql = %s", querySql), e);
        }
        return results;
    }

    private Connection getConnection() throws SQLException {
        return prestoConnection.getConnection();
    }
}
