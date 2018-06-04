package com.zjb.app.spark.project.utils

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object DateUtil {

  val INPUT_TIME_FORMAT = "yyyy-MM-dd HH:mm:SS"

  val inputDateTimeFormatter = DateTimeFormat.forPattern(INPUT_TIME_FORMAT)

  val OUTPUT_TIME_FORMAT = "yyyyMMddHHmm"

  def getTime(time: String) = {
    DateTime.parse(time, inputDateTimeFormatter)
  }

  def parseToMinute(time: String) = {
    getTime(time).toString(OUTPUT_TIME_FORMAT)
  }
}
