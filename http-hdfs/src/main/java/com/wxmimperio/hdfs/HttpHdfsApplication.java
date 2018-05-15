package com.wxmimperio.hdfs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@EnableAutoConfiguration
@SpringBootApplication
@ComponentScan
@ServletComponentScan
@EnableSwagger2
public class HttpHdfsApplication {
    public static ApplicationContext context;

    public static void main(String[] args) {
        context = SpringApplication.run(HttpHdfsApplication.class, args);
    }
}
