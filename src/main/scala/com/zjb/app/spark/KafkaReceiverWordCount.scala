package com.zjb.app.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming 对接kafka 方式一：Receiver
  */
object KafkaReceiverWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.print("Usage KafkaReceiverWordCount <zkQuorum> <groupId> <topics> <numPartitions>")
      System.exit(1)
    }

    val Array(zkQuorum, groupId, topics, numPartitions) = args

    val sparkConf = new SparkConf()
    //.setAppName("KafkaReceiverWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // kafkaStream generate
    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics.split(",").map((_, numPartitions.toInt)).toMap);
    kafkaStream.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
