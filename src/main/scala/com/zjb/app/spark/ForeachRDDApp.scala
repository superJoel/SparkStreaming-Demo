package com.zjb.app.spark

import java.sql.{Connection, DriverManager}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * foreachRDD 输出数据到外部数据源MySql
  */
object ForeachRDDApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ForeachRDDApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999)
    val res = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //    res.foreachRDD(rdd => {
    //      val connection = getConnection() // executed at driver
    //      rdd.foreach(record => {
    //        val sql = "insert into spark_test.word_count(word, wordCount) values('" + record._1 + "'," + record._2 + ")"
    //        connection.prepareStatement(sql).execute()
    //      })
    //      connection.close()
    //    })
    res.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        // TODO 换成线程池
        val connection = getConnection()
        partition.foreach(record => {
          val sql = "insert into spark_test.word_count(word, wordCount) values('" + record._1 + "'," + record._2 + ")"
          connection.prepareStatement(sql).execute()
        })
        connection.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def getConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://118.25.25.65:3306/spark_test", "root", "oneinstack")
  }
}
