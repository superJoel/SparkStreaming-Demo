package com.zjb.app.spark

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Spark Streaming 对接Kafka 方式二：Direct
  */
object KafkaDirectWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.print("Usage KafkaDirectWordCount <brokerList> <topics>")
      System.exit(1)
    }

    val Array(brokerList, topics) = args

    val sparkConf = new SparkConf()
      .setAppName("KafkaDirectWordCount").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList)

    // kafkaStream generate
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,
      kafkaParams, topics.split(",").toSet)
    kafkaStream.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
