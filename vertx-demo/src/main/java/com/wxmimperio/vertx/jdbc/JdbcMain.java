package com.wxmimperio.vertx.jdbc;

import com.wxmimperio.vertx.verticle.MyVerticle;
import io.vertx.core.Vertx;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className JdbcMain.java
 * @description This is the description of JdbcMain.java
 * @createTime 2020-10-26 16:31:00
 */
public class JdbcMain {

    public static void main(String[] args) {
        final Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(MyVerticle.class.getName());
    }
}
