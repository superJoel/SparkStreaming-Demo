package com.zjb.app.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Receiver InputDStream
  */
object NetworkWordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999)
    val res = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    res.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
