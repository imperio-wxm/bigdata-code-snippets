package com.wxmimperio.file

import java.time.LocalDateTime

import com.wxmimperio.file.common.{CommonUtils, SchemaHelper}
import org.apache.avro.Schema
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object FileSplitMain {
    val logger: Logger = LogManager.getLogger("FileSplitMain")

    def main(args: Array[String]): Unit = {

        val params = CommonUtils.getParams(args)

        // /wxm/spark_file/all.txt
        val inputPath = params(0).toString
        // /wxm/spark_file/output/
        val outPath = params(1).toString
        val startTime = params(2).asInstanceOf[LocalDateTime]
        val endTime = params(3).asInstanceOf[LocalDateTime]

        val conf = new SparkConf().setAppName("SparkFileSplit").set("spark.network.timeout", "1200")
        val sc = SparkSession.builder.config(conf).getOrCreate().sparkContext
        val broadcast = sc.broadcast(SchemaHelper.getAllCacheSchema())

        sc.textFile(inputPath)
          .map(line => parserData(line, startTime, endTime, broadcast))
          .filter(newLine => StringUtils.isNotEmpty(newLine._1.toString) || StringUtils.isNotEmpty(newLine._2.toString))
          .saveAsHadoopFile(outPath, classOf[Text], classOf[Text], classOf[SplitMultipleSequenceFileOutputFormat[_, _]])

        sc.stop()
    }

    def parserData(line: String, startTime: LocalDateTime, endTime: LocalDateTime, broadcast: Broadcast[mutable.HashMap[String, Schema]]): (Text, Text) = {
        var result = (new Text(), new Text())
        try {
            if (StringUtils.isNotEmpty(line.toString)) {
                val newValue = line.split("\\|", -1)
                val tableName = newValue(0)
                val subValue = newValue.slice(1, newValue.size)
                val eventTimeValue = CommonUtils.getDataEventTime(broadcast, subValue, tableName)
                if (!CommonUtils.needFilter(startTime, endTime, eventTimeValue)) {
                    val key = CommonUtils.getMsgKey(tableName, CommonUtils.getPartDate(eventTimeValue))
                    val value = subValue.mkString("\t")
                    result = (new Text(key), new Text(value))
                }
            }
        } catch {
            case e: Exception => logger.error(e)
        }
        result
    }
}

class SplitMultipleSequenceFileOutputFormat[K, V] extends MultipleSequenceFileOutputFormat[K, V]() {

    override def generateActualKey(key: K, value: V): K = {
        new Text(key.toString.split("\\|", -1)(2)).asInstanceOf[K]
    }

    override def generateFileNameForKeyValue(key: K, value: V, name: String): String = {
        val keyArray = key.toString.split("\\|", -1)
        keyArray(0) + "/part_date=" + keyArray(1) + "/" + name
    }
}