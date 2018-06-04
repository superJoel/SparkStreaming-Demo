package com.zjb.app.spark.project.spark

import com.zjb.app.spark.project.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import com.zjb.app.spark.project.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.zjb.app.spark.project.utils.DateUtil
import kafka.serializer.StringDecoder
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * 流式统计
  */
object StatStreamingApp {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      print("StatStreamingApp Usage: <brokerList> <topics>")
      System.exit(1)
    }

    val Array(brokerList, topics) = args

    val sparkConf = new SparkConf()
    //      .setMaster("local").setAppName("StatStreamingApp")
    val ssc = new StreamingContext(sparkConf, Minutes(1))

    val kafkaParams = Map("metadata.broker.list" -> brokerList)
    val topicSet = topics.split(",").toSet

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    // 清洗数据
    val cleanData = messages.map(_._2).map(msg => {
      //      "GET /class/205.html HTTP/1.1"
      val fields = msg.split("\t");
      var courseId = 0
      val requestEle = fields(2).split(" ")(1).split("/")
      if (requestEle(1).startsWith("class")) {
        courseId = requestEle(2).substring(0, requestEle(2).lastIndexOf(".")).toInt
      }
      ClickLog(fields(1), DateUtil.parseToMinute(fields(0)), courseId, fields(4).toInt, fields(3))
    }).filter(_.courseId != 0)
    //      .print()

    // 统计实战课程的日点击量，并存储到HBase
    cleanData.map(cl => (cl.time.substring(0, 8) + "_" + cl.courseId, 1L)).reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(parti => {
          val resList = new ListBuffer[CourseClickCount]
          parti.foreach(pair => {
            resList.append(CourseClickCount(pair._1, pair._2))
          })
          CourseClickCountDAO.save(resList)
        })
      })

    // 统计从搜索引擎引流过来的课程日点击量，并存储到HBase
    cleanData.map(cl => {
      val httpRefer = cl.httpRefer
      val splitedRefers = httpRefer.replaceAll("//", "/").split("/")
      var host = ""
      if (splitedRefers.length > 2) {
        host = splitedRefers(1)
      }
      (cl.time, host, cl.courseId)
    }).filter(_._2 != "")
      .map(x => (StringUtils.join(Array[AnyRef](x._1.substring(0, 8), x._2, x._3.toString), '_'), 1L))
      .reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(parti => {
          val resList = new ListBuffer[CourseSearchClickCount]
          parti.foreach(pair => {
            resList.append(CourseSearchClickCount(pair._1, pair._2))
          })
          CourseSearchClickCountDAO.save(resList)
        })
      })

    ssc.start()
    ssc.awaitTermination()
  }
}
