package com.zjb.app.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * File Dstream
  */
object FileWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("FileWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))

    val lines = ssc.textFileStream("/Users/Joel/data/ss")
    val res = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    res.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
