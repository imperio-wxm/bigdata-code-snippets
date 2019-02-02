package com.wxmimperio.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.io.File;

public class HiveTables {

    public static void main(String[] args) {

        String dwPath = args[0];
        String dataPath = args[1];

        // Configuration of Hive is done by placing your hive-site.xml, core-site.xml (for security configuration),
        // and hdfs-site.xml (for HDFS configuration) file in conf/.
        SparkConf sparkConf = new SparkConf();

        // 创建一个本地的hive metaStore
        // warehouseLocation points to the default location for managed databases and tables
        String warehouseLocation = new File(dwPath).getAbsolutePath();
        sparkConf.set("spark.sql.warehouse.dir", warehouseLocation);

        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .appName("Java Spark SQL basic example")
                .master("local")
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("CREATE TABLE IF NOT EXISTS person (key INT, value STRING) PARTITIONED BY (part_date string) row format delimited fields terminated by ',' STORED AS TEXTFILE");
        spark.sql("LOAD DATA LOCAL INPATH '" + dataPath + "' OVERWRITE INTO TABLE person PARTITION (part_date='2019-02-01')");
        spark.sql("SELECT * FROM person").show();
    }
}
