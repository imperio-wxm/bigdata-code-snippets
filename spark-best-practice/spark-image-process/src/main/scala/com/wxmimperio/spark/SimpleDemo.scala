package com.wxmimperio.spark

import java.awt.image.{BufferedImage, WritableRaster}
import java.io.{ByteArrayInputStream, File}

import javax.imageio.ImageIO
import org.apache.commons.io.FilenameUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SimpleDemo {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local")
        val spark = SparkSession.builder()
          .config(conf)
          .getOrCreate()

        val imageDF = spark.read
          .format("image")
          .load("E:\\coding\\github\\hadoop-code-snippets\\spark-best-practice\\spark-image-process\\src\\main\\resources\\pngs\\")

        imageDF.printSchema()

        val row = imageDF.select(
            "image.origin",
            "image.width",
            "image.height",
            "image.nChannels",
            "image.mode",
            "image.data"
        )

        /*row.foreach(row => {
            val data = row.getAs[Array[Byte]]("data")
            println(data)
        })*/

        row.repartition(1).rdd.map(row => {
            val origin = row.getAs[String]("origin")
            println(origin)
            val data = row.getAs[Array[Byte]]("data")
            val width = row.getAs[Int]("width")
            val height = row.getAs[Int]("height")


            val image = new BufferedImage(width, height, BufferedImage.TYPE_INT_BGR)
            val raster = image.getData.asInstanceOf[WritableRaster]
            val pixels = data.map(_.toDouble)
            raster.setPixels(0, 0, width, height, pixels)
            image.setData(raster)

            for (i <- image.getMinX until image.getWidth()) {
                for (j <- image.getMinY until image.getHeight()) {
                    println(image.getRGB(i, j))
                }
            }

            val buffImg = new BufferedImage(width, height, BufferedImage.TYPE_3BYTE_BGR)
            val g = buffImg.createGraphics
            g.drawImage(image, 0, 0, null)
            g.dispose()

            (origin, buffImg)
        }).coalesce(1).foreach(buffer => {
            val output = "E:\\coding\\github\\hadoop-code-snippets\\spark-best-practice\\spark-image-process\\src\\main\\resources\\result\\" + FilenameUtils.getName(buffer._1)
            ImageIO.write(buffer._2, "png", new File(output))
        })

        Thread.sleep(100000)
    }
}