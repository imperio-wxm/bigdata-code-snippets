package com.wxmimperio.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class JdbcTables {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();

        String mysqlUrl = "jdbc:mysql://127.0.0.1:3306/wxm?useUnicode=true&characterEncoding=utf-8";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "password");

        Dataset<Row> dataSet = spark.read().jdbc(mysqlUrl, "wxm.user", connectionProperties);
        /**
         * +---+-----+---+------+
         * | id| name|age|gender|
         * +---+-----+---+------+
         * |161|wxm_0|  0|     男|
         * |162|wxm_1|  1|     男|
         * |163|wxm_2|  2|     男|
         * |164|wxm_3|  3|     男|
         * |165|wxm_4|  4|     男|
         * |166|wxm_5|  5|     男|
         * |167|wxm_6|  6|     男|
         * |168|wxm_7|  7|     男|
         * |169|wxm_8|  8|     男|
         * |170|wxm_9|  9|     男|
         * +---+-----+---+------+
         */
        dataSet.limit(10).show();

        List<User> users = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            users.add(new User("wxm_" + i, i, "男"));
        }

        // 在SaveMode.Overwrite模式下，先truncate表所有数据，再将新数据插入
        // connectionProperties.put("truncate", "true");
        Dataset<Row> saveUsers = spark.createDataFrame(users, User.class);
        saveUsers.write().mode(SaveMode.Append).jdbc(mysqlUrl, "wxm.user", connectionProperties);
    }

    public static class User {
        private Integer id;
        private String name;
        private Integer age;
        private String gender;

        public User() {
        }


        public User(String name, Integer age, String gender) {
            this.name = name;
            this.age = age;
            this.gender = gender;
        }

        public User(Integer id, String name, Integer age, String gender) {
            this.id = id;
            this.name = name;
            this.age = age;
            this.gender = gender;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        public String getGender() {
            return gender;
        }

        public void setGender(String gender) {
            this.gender = gender;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return "User{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", age=" + age +
                    ", gender='" + gender + '\'' +
                    '}';
        }
    }
}
