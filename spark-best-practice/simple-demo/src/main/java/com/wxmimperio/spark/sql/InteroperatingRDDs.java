package com.wxmimperio.spark.sql;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class InteroperatingRDDs {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();

        String txtPath = "simple-demo/src/resources/people.txt";

        JavaRDD<SparkDataFrameDataSetSql.Person> javaPairRDD = spark.read().textFile(txtPath).toJavaRDD().map((Function<String, SparkDataFrameDataSetSql.Person>) value -> {
            String[] values = value.split("\\|", -1);
            return new SparkDataFrameDataSetSql.Person(
                    StringUtils.isEmpty(values[0]) ? null : values[0],
                    StringUtils.isEmpty(values[1]) ? null : Integer.valueOf(values[1])
            );
        });

        // JavaRDD to DataSet
        // 使用javaBean反射，构建DataSet
        Dataset<Row> personDS = spark.createDataFrame(javaPairRDD, SparkDataFrameDataSetSql.Person.class);
        personDS.show();

        personDS.createOrReplaceTempView("person");
        Dataset<Row> wxmPerson = spark.sql("select * from person where name = 'wxm'");
        wxmPerson.show();

    }
}
