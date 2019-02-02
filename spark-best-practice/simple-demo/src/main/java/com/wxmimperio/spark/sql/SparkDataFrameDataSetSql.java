package com.wxmimperio.spark.sql;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class SparkDataFrameDataSetSql {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();

        String jsonPath = "simple-demo/src/resources/people.json";

        // dataFrame ops
        Dataset<Row> df = spark.read().json(jsonPath);
        df.show();
        df.printSchema();
        df.select("name").show();
        df.select(df.col("name"), df.col("age").plus(100).as("agePlus")).show();

        // sql ops
        df.createOrReplaceTempView("people");
        Dataset<Row> sqlDF = spark.sql("SELECT count(*),name FROM people group by name");
        sqlDF.show();

        // dataSet ops
        // Create an instance of a Bean class
        List<Person> personList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            personList.add(new Person("Andy-" + i, 32 + i));
        }
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                personList,
                personEncoder
        );
        javaBeanDS.show();

        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> integerDS = spark.createDataset(
                Arrays.asList(1, 2, 3, 4, 5),
                integerEncoder
        );
        integerDS = integerDS.filter((FilterFunction<Integer>) value -> value > 2)
                .map((MapFunction<Integer, Integer>) value -> value + 10, integerEncoder);
        integerDS.show();

        // read dataset from jsonFile
       spark.read().json(jsonPath).as(personEncoder).show();
    }

    public static class Person implements Serializable {
        private String name;
        private Integer age;


        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
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

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
}
