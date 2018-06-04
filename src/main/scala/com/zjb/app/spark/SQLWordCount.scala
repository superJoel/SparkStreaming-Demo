package com.zjb.app.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * Spark Streaming 集成 Spark Sql
  */
object SQLWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SQLWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val lines = ssc.socketTextStream("localhost", 9999)

    lines.flatMap(_.split(" ")).foreachRDD(rdd => {
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._
      val wordDF = rdd.map(word => Word(word)).toDF()
      wordDF.createOrReplaceTempView("words")
      val res = spark.sql("select word,count(1) from words group by word")
      res.show()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

/**
  * 单例
  */
object SparkSessionSingleton {

  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}

case class Word(word: String)
