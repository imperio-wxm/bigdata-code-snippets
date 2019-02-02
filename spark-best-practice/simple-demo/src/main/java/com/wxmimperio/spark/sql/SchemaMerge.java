package com.wxmimperio.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SchemaMerge {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();

        List<Person> personList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            personList.add(new Person("name_" + i, i));
        }

        List<Computer> computerList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            computerList.add(new Computer("cpu_" + i, i * 1024));
        }

        Dataset<Row> personDataSet = spark.createDataFrame(personList, Person.class);
        personDataSet.show();
        personDataSet.write().mode(SaveMode.Append).format("parquet").save("result/test_table/key=1");

        Dataset<Row> computerDataSet = spark.createDataFrame(computerList, Computer.class);
        computerDataSet.show();
        computerDataSet.write().mode(SaveMode.Append).format("parquet").save("result/test_table/key=2");

        Dataset<Row> mergedDF = spark.read().option("mergeSchema", true).parquet("result/test_table");
        mergedDF.printSchema();

        // 只是schema 合并，数据并没有
        mergedDF.select("age", "memory", "key").show();
    }

    public static class Person implements Serializable {
        private String name;
        private Integer age;

        public Person() {
        }

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
                    ", age='" + age + '\'' +
                    '}';
        }
    }

    public static class Computer implements Serializable {
        private String cup;
        private Integer memory;

        public Computer() {
        }

        public Computer(String cup, Integer memory) {
            this.cup = cup;
            this.memory = memory;
        }

        public String getCup() {
            return cup;
        }

        public void setCup(String cup) {
            this.cup = cup;
        }

        public Integer getMemory() {
            return memory;
        }

        public void setMemory(Integer memory) {
            this.memory = memory;
        }

        @Override
        public String toString() {
            return "Computer{" +
                    "cup='" + cup + '\'' +
                    ", memory=" + memory +
                    '}';
        }
    }
}
