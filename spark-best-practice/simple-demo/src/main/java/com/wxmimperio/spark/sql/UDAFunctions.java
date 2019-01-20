package com.wxmimperio.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class UDAFunctions {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();

        // Untyped
        spark.udf().register("myAverage", new MyAverage());
        Dataset<Row> df = spark.read().json("simple-demo/src/resources/people.json");
        df.createOrReplaceTempView("people");
        df.show();

        Dataset<Row> result = spark.sql("SELECT myAverage(age) as average FROM people");
        result.show();
    }

    public static class MyAverage extends UserDefinedAggregateFunction {
        private StructType inputSchema;
        private StructType bufferSchema;

        public MyAverage() {
            List<StructField> inputFields = new ArrayList<>();
            inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.LongType, true));
            inputSchema = DataTypes.createStructType(inputFields);

            List<StructField> bufferFields = new ArrayList<>();
            bufferFields.add(DataTypes.createStructField("sum", DataTypes.LongType, true));
            bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
            bufferSchema = DataTypes.createStructType(bufferFields);
        }

        @Override
        public StructType inputSchema() {
            return inputSchema;
        }

        @Override
        public StructType bufferSchema() {
            return bufferSchema;
        }

        @Override
        public DataType dataType() {
            return DataTypes.DoubleType;
        }

        @Override
        public boolean deterministic() {
            return true;
        }

        @Override
        public void initialize(MutableAggregationBuffer mutableAggregationBuffer) {
            mutableAggregationBuffer.update(0, 0L);
            mutableAggregationBuffer.update(1, 0L);
        }

        @Override
        public void update(MutableAggregationBuffer mutableAggregationBuffer, Row row) {
            if (!row.isNullAt(0)) {
                long updatedSum = mutableAggregationBuffer.getLong(0) + row.getLong(0);
                long updatedCount = mutableAggregationBuffer.getLong(1) + 1;
                mutableAggregationBuffer.update(0, updatedSum);
                mutableAggregationBuffer.update(1, updatedCount);
            }
        }

        @Override
        public void merge(MutableAggregationBuffer mutableAggregationBuffer, Row row) {
            long mergedSum = mutableAggregationBuffer.getLong(0) + row.getLong(0);
            long mergedCount = mutableAggregationBuffer.getLong(1) + row.getLong(1);
            mutableAggregationBuffer.update(0, mergedSum);
            mutableAggregationBuffer.update(1, mergedCount);
        }

        @Override
        public Object evaluate(Row row) {
            return ((double) row.getLong(0)) / row.getLong(1);
        }
    }
}
