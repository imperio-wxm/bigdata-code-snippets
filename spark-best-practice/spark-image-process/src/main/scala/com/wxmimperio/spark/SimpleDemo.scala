package com.wxmimperio.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SimpleDemo {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]")
        val spark = SparkSession.builder()
          .config(conf)
          .getOrCreate()

        val imageDF = spark.read
          .format("image")
          .load("E:\\coding\\github\\hadoop-code-snippets\\spark-best-practice\\spark-image-process\\src\\main\\resources\\gorilla_PNG18712.png")

        imageDF.printSchema()

        val row = imageDF.select(
            "image.origin",
            "image.width",
            "image.height",
            "image.nChannels",
            "image.mode",
            "image.data"
        )

        row.foreach(row => {
            val data = row.getAs[Array[Byte]]("data")
            println(data)
        })

    }
}
