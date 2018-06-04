package com.zjb.app.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 基于push的Flume、Spark Streaming集成
  */
object FlumeNetWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.print("Usage FlumeNetWordCount <hostname> <port>")
      System.exit(1)
    }

    val Array(hostname, port) = args

    val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val events = FlumeUtils.createStream(ssc, hostname, port.toInt)
    val res = events.map(e => new String(e.event.getBody.array()).trim)
      .flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    res.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
