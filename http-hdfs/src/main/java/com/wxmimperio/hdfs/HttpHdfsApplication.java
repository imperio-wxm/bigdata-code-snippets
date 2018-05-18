package com.wxmimperio.hdfs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@EnableAutoConfiguration
@SpringBootApplication
@ComponentScan
@EnableSwagger2
@EnableScheduling
public class HttpHdfsApplication {
    public static ApplicationContext context;

    public static void main(String[] args) {
        context = SpringApplication.run(HttpHdfsApplication.class, args);
    }
}
