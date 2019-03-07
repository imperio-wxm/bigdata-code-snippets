package com.wxmimperio.presto.connection;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.wxmimperio.presto.config.PrestoConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public class PrestoConnection {
    private static final Logger LOG = Logger.getLogger(PrestoConnection.class);

    private PrestoConfig prestoConfig;
    private HikariDataSource hikariDataSource;
    private HealthCheckRegistry healthCheckRegistry;
    private static final long DEFAULT_CHECKHEALTH_PURGER_INTERVAL = 60 * 1000 * 30L;

    @Autowired
    public PrestoConnection(PrestoConfig prestoConfig) {
        this.prestoConfig = prestoConfig;
    }

    @PostConstruct
    private void initConnect() {
        MetricRegistry metricRegistry = new MetricRegistry();
        this.healthCheckRegistry = new HealthCheckRegistry();
        this.hikariDataSource = new HikariDataSource();
        this.hikariDataSource.setDriverClassName(prestoConfig.getDriverClassName());
        this.hikariDataSource.setJdbcUrl(prestoConfig.getUrl());
        this.hikariDataSource.setUsername(prestoConfig.getUserName());
        this.hikariDataSource.setPassword(prestoConfig.getPassWord());
        this.hikariDataSource.setIdleTimeout(prestoConfig.getIdleTimeout());
        this.hikariDataSource.setMaxLifetime(prestoConfig.getMaxLifetime());
        this.hikariDataSource.setMinimumIdle(prestoConfig.getMinimumIdle());
        this.hikariDataSource.setMaximumPoolSize(prestoConfig.getMaximumPoolSize());
        this.hikariDataSource.setValidationTimeout(prestoConfig.getValidationTimeout());
        this.hikariDataSource.setLeakDetectionThreshold(prestoConfig.getLeakDetectionThreshold());
        this.hikariDataSource.setPoolName(prestoConfig.getPoolName());
        this.hikariDataSource.setMetricRegistry(metricRegistry);
        this.hikariDataSource.setHealthCheckRegistry(healthCheckRegistry);
        final Slf4jReporter reporter = Slf4jReporter.forRegistry(metricRegistry)
                .outputTo(LoggerFactory.getLogger("com.zaxxer.hikari.metrics"))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(10, TimeUnit.MINUTES);
    }

    @Scheduled(fixedRate = DEFAULT_CHECKHEALTH_PURGER_INTERVAL)
    public void checkHealth() {
        final Map<String, HealthCheck.Result> results = healthCheckRegistry.runHealthChecks();
        for (Map.Entry<String, HealthCheck.Result> entry : results.entrySet()) {
            if (entry.getValue().isHealthy()) {
                LOG.info(entry.getKey() + " is healthy");
            } else {
                LOG.error(entry.getKey() + " is UNHEALTHY: " + entry.getValue().getMessage());
                final Throwable e = entry.getValue().getError();
                if (e != null) {
                    throw new RuntimeException("Presto connection pool is not health.", e);
                }
            }
        }
    }

    public Connection getConnection() throws SQLException {
        return hikariDataSource.getConnection();
    }
}
