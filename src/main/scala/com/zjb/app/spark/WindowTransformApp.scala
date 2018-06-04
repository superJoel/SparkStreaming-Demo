package com.zjb.app.spark

import com.zjb.app.spark.StatefulWordCount.updateFunction
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * window算子操作
  */
object WindowTransformApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("WindowTransformApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999)
    val res = lines.flatMap(_.split(" ")).map((_, 1)).window(Seconds(30), Seconds(10)).reduceByKey(_ + _)
    res.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
