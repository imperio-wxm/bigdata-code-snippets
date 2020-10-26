package com.wxmimperio.vertx.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className MyVerticle.java
 * @description This is the description of MyVerticle.java
 * @createTime 2020-10-26 16:28:00
 */
public class MyVerticle extends AbstractVerticle {

    private JDBCClient jdbcClient;
    private static final String API_GET = "/test/:id";
    private static final String HTTP_HOST = "127.0.0.1";
    private static final int HTTP_PORT = 8080;

    @Override
    public void init(Vertx vertx, Context context) {
        super.init(vertx, context);
        initJDBCDateSource();
    }

    private void initJDBCDateSource() {
        System.out.println("连接测试:::--------");
        jdbcClient = JDBCClient.createShared(vertx, config());
        jdbcClient.getConnection(conn -> {
            if (conn.failed()) {
                System.out.println("数据库初始化连接是失败");
                System.err.println(conn.cause().getMessage());
                return;
            }
            // 获取连接
            final SQLConnection connection = conn.result();
            connection.query("select * from warship.app_info limit 10", result -> {
                if (result.failed()) {
                    throw new RuntimeException("查询失败");
                }
                final List<JsonObject> rows = result.result().getRows();
                for (JsonArray line : result.result().getResults()) {
                    System.out.println("== : " + line.getList().toArray());
                }
            });
            //执行 [创建一张test表]
            /*connection.execute("create table IF NOT  EXISTS test(id int primary key, name varchar(255))", res -> {
                //如果建表失败
                if (res.failed()) {
                    //抛出异常[可自定义]
                    throw new RuntimeException(res.cause());
                }
                // 插入一条数据
                connection.execute("insert into test values(1, 'Hello')", insert -> {
                    // 查询数据
                    connection.query("select * from test", rs -> {
                        //打印结果
                        System.out.println("数据库初始化成功");
                        for (JsonArray line : rs.result().getResults()) {
                            System.out.println(line.encode());
                        }
                        // 最后关闭连接
                        connection.close(done -> {
                            //如果连接释放失败
                            if (done.failed()) {
                                //抛出异常
                                throw new RuntimeException(done.cause());
                            }
                        });
                    });
                });
            });*/
        });
    }

    @Override
    public JsonObject config() {
        super.config();
        return new JsonObject()
                .put("url", "jdbc:clickhouse://10.221.50.169:8123/warship")
                .put("driver_class", "ru.yandex.clickhouse.ClickHouseDriver")
                .put("user", "default")
                .put("password", "123")
                .put("max_pool_size", 10);
    }

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        router.get(API_GET).handler(this::handleGet);
        //创建一个服务
        vertx.createHttpServer().requestHandler(router::accept).listen(HTTP_PORT, HTTP_HOST, result -> {
            if (result.succeeded()) {
                startFuture.complete();
            } else {
                startFuture.fail(result.cause());
            }
        });
    }

    private void handleGet(RoutingContext routingContext) {
        String id = routingContext.request().getParam("id");
        final HttpServerResponse response = getHttpServerResponse(routingContext);
        final SQLClient connection = jdbcClient.getConnection(res -> {
            if (res.failed()) {
                response.setStatusCode(400);
                response.end("服务器忙,请联系管理员!!!!");
                throw new RuntimeException(res.cause().getMessage());
            }
            final SQLConnection sqlConnection = res.result();
            sqlConnection.query("select * from warship.app_info limit 10", result -> {
                if (result.failed()) {
                    throw new RuntimeException("查询失败" + id);
                }
                final List<JsonObject> rows = result.result().getRows();
                for (JsonArray line : result.result().getResults()) {
                    System.out.println(line.encode());
                }
                response.setStatusCode(200);
                response.end(rows.toString());
            });
        });
    }

    public HttpServerResponse getHttpServerResponse(RoutingContext rtx) {
        HttpServerResponse response = rtx.response();
        response.setChunked(true);
        response.putHeader("content-type", "application/json;charset=UTF-8");
        return response;
    }
}
