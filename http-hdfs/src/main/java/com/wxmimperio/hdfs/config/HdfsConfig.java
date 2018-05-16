package com.wxmimperio.hdfs.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties(prefix = "xml.site")
@Component
public class HdfsConfig {

    private String core;
    private String hdfs;

    public String getCore() {
        return core;
    }

    public void setCore(String core) {
        this.core = core;
    }

    public String getHdfs() {
        return hdfs;
    }

    public void setHdfs(String hdfs) {
        this.hdfs = hdfs;
    }

    @Override
    public String toString() {
        return "HdfsConfig{" +
                "core='" + core + '\'' +
                ", hdfs='" + hdfs + '\'' +
                '}';
    }
}
