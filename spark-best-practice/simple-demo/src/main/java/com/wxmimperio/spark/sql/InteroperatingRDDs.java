package com.wxmimperio.spark.sql;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InteroperatingRDDs {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();

        String txtPath = "simple-demo/src/resources/people.txt";

        // 使用javaBean反射，构建DataSet
        JavaRDD<SparkDataFrameDataSetSql.Person> javaPairRDD = spark.read().textFile(txtPath).toJavaRDD().map((Function<String, SparkDataFrameDataSetSql.Person>) value -> {
            String[] values = value.split("\\|", -1);
            return new SparkDataFrameDataSetSql.Person(
                    StringUtils.isEmpty(values[0]) ? null : values[0],
                    StringUtils.isEmpty(values[1]) ? null : Integer.valueOf(values[1])
            );
        });

        // JavaRDD to DataSet
        Dataset<Row> personDS = spark.createDataFrame(javaPairRDD, SparkDataFrameDataSetSql.Person.class);
        personDS.show();

        personDS.createOrReplaceTempView("person");
        Dataset<Row> wxmPerson = spark.sql("select * from person where name = 'wxm'");
        wxmPerson.show();

        /////////////////////////////////////

        // 编程方式指定Schema，构建DataSet
        // Create an RDD
        JavaRDD<String> peopleRDD = spark.sparkContext()
                .textFile(txtPath, 1)
                .toJavaRDD();

        // 先自定义schema
        // The schema is encoded in a string
        String schemaString = "name,age";
        // Generate the schema based on the string of schema
        List<StructField> fields = Lists.newArrayList();
        for (String fieldName : schemaString.split(",", -1)) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
            String[] attributes = record.split("\\|", -1);
            String name = StringUtils.isEmpty(attributes[0]) ? null : attributes[0];
            String age = StringUtils.isEmpty(attributes[1]) ? null : attributes[1];
            return RowFactory.create(name, age);
        });
        // 绑定schema 与数据集
        // Apply the schema to the RDD
        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);
        // Creates a temporary view using the DataFrame
        peopleDataFrame.createOrReplaceTempView("people");
        // SQL can be run over a temporary view created using DataFrames
        Dataset<Row> results = spark.sql("SELECT * FROM people");
        results.show();
    }
}
