package com.wxmimperio.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class MultipleDataSourceJoin {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();

        String mysqlUrl = "jdbc:mysql://127.0.0.1:3306/wxm?useUnicode=true&characterEncoding=utf-8";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "password");

        Dataset<Row> userDataSet = spark.read().jdbc(mysqlUrl, "wxm.user", connectionProperties);
        userDataSet.createOrReplaceTempView("user");
        Dataset<Row> userRow = spark.sql("select * from user");
        userRow.show();

        Dataset<Row> stuDataSet = spark.read().jdbc(mysqlUrl, "wxm.student", connectionProperties);
        stuDataSet.createOrReplaceTempView("student");
        Dataset<Row> stuRow = spark.sql("select * from student");
        stuRow.show();

        String jsonPath = "D:\\d_backup\\github\\hadoop-code-snippets\\spark-best-practice\\simple-demo\\src\\resources\\people.json";

        // format: json, parquet, jdbc, orc, libsvm, csv, text
        Dataset<Row> dataset = spark.read().format("json").load(jsonPath).as("people");
        dataset.createOrReplaceTempView("people");
        dataset.show();

        // 两个mysql 的不同表join，再和json文件数据进行join
        //Dataset<Row>  joinRow = spark.sql("select count(s.age),s.age,u.id,u.name from user u inner join student s on u.name = s.name group by s.age,u.id,u.name order by s.age,u.id ");
        Dataset<Row> joinRow = spark.sql("select * from (select count(s.age),s.age,u.id,u.name from user u inner join student s on u.name = s.name group by s.age,u.id,u.name order by s.age,u.id) a left join people on a.name = people.name where people.name is not null");
        joinRow.show();
    }
}
