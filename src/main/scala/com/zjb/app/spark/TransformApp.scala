package com.zjb.app.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 黑名单过滤
  *
  *
  *
  * 测试数据
  * 20180515,zs
    20180515,ls
    20180515,ww
    20180515,zl

    zs
    ls
  */
object TransformApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TransformApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 黑名单rdd
    val blackList = List("zs", "ls")
    val blackRdd = ssc.sparkContext.parallelize(blackList).map((_, true))

    val lines = ssc.socketTextStream("localhost", 9999)
    // 过滤黑名单
    val res = lines.map(x => (x.split(',')(1), x)).transform(rdd => {
      rdd.leftOuterJoin(blackRdd).filter(!_._2._2.getOrElse(false))
    }).map(_._2._1)
    res.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
