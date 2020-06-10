package com.wxmimperio.file.common

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

import com.wxmimperio.file.exception.SparkFileSplitException
import org.apache.avro.Schema
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable
import scala.util.control.Breaks._
import scala.collection.JavaConversions._

object CommonUtils {
    val logger: Logger = LogManager.getLogger("CommonUtils")

    val eventTimePattern: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val partDatePattern: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    def needFilter(startTime: LocalDateTime, endTime: LocalDateTime, eventTime: String): Boolean = {
        val localEventTime = LocalDateTime.from(CommonUtils.eventTimePattern.parse(eventTime))
        // 前闭后开 ['2019-01-07 18:20:40','2019-01-07 19:20:20')
        // event_time 小于 start 或者 大于 end，过滤掉不写入
        (null != startTime && null != endTime) && (localEventTime.isAfter(endTime.plusSeconds(-1)) || localEventTime.isBefore(startTime))
    }

    def getPartDate(evenTime: String): String = partDatePattern.format(eventTimePattern.parse(evenTime))

    def getMsgKey(tableName: String, partDate: String): String = tableName + "|" + partDate + "|" + UUID.randomUUID().toString.replaceAll("-", "") + System.currentTimeMillis()

    def getDataEventTime(broadcast: Broadcast[mutable.HashMap[String, Schema]], newValue: Array[String], tableName: String): String = {
        if (broadcast.value.contains(tableName)) {
            val schema = broadcast.value(tableName)
            var evenTimePos = 0
            breakable {
                for (field <- schema.getFields) {
                    if ("event_time".equals(field.name)) {
                        break
                    }
                    evenTimePos += 1
                }
            }
            newValue(evenTimePos)
        } else {
            throw new SparkFileSplitException(s"Schema $tableName can not found.")
        }
    }

    def getParams(args: Array[String]): Array[Any] = {
        var result = new Array[Any](4)
        try {
            if (args.length != 4) {
                throw new SparkFileSplitException("Must have four parameters, input|output|start|end.")
            }
            logger.info("=============================")
            logger.info("inputPath = " + args(0))
            logger.info("outputPath = " + args(1))
            logger.info("startTime = " + args(2))
            logger.info("endTime = " + args(3))
            logger.info("=============================")
            val inputPath = args(0)
            val outputPath = args(1)
            val startTime = LocalDateTime.from(eventTimePattern.parse(args(2)))
            val endTime = LocalDateTime.from(eventTimePattern.parse(args(3)))
            if (startTime.isAfter(endTime)) {
                throw new SparkFileSplitException("Start time should be less than end time.")
            }
            result = Array(inputPath, outputPath, startTime, endTime)
        } catch {
            case exception: Exception => logger.error(exception)
                println(exception)
                System.exit(1)
        }
        result
    }
}
