package com.zjb.app.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * UpdateStateByKey操作
  * 常用于状态累计
  */
object StatefulWordCount {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sc, Seconds(5))

    // 使用UpdateStateByKey必须checkpoint 以前的状态
    // 生产环境中最好设置为hdfs目录
    ssc.checkpoint(".")
    val lines = ssc.socketTextStream("localhost", 9999)
    val res = lines.flatMap(_.split(" ")).map((_, 1))
    val state = res.updateStateByKey(updateFunction _)
    state.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val curSum = currentValues.sum
    val pre = preValues.getOrElse(0)
    Some(pre + curSum)
  }
}
