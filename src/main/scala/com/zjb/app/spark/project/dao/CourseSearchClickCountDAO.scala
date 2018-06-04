package com.zjb.app.spark.project.dao

import com.zjb.app.spark.project.domain.{CourseClickCount, CourseSearchClickCount}
import com.zjb.app.spark.project.utils.HBaseUtil
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * CourseClickCount数据层
  */
object CourseSearchClickCountDAO {

  val tableName = "course_search_clickcount";
  val familyName = "info";
  val qualifier = "click_count";

  /**
    * 保存天课程点击数
    *
    * @param list
    */
  def save(list: ListBuffer[CourseSearchClickCount]): Unit = {
    val table = HBaseUtil.getInstance().getHTable(tableName)
    list.foreach(cscc => {
      table.incrementColumnValue(Bytes.toBytes(cscc.day_search_course), Bytes.toBytes(familyName), Bytes.toBytes(qualifier),
        cscc.clickcount)
    })
  }
}
