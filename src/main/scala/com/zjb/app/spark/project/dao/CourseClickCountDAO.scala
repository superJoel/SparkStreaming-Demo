package com.zjb.app.spark.project.dao

import com.zjb.app.spark.project.domain.CourseClickCount
import com.zjb.app.spark.project.utils.HBaseUtil
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * CourseClickCount数据层
  */
object CourseClickCountDAO {

  val tableName = "course_clickcount";
  val familyName = "info";
  val qualifier = "click_count";

  /**
    * 保存天课程点击数
    *
    * @param list
    */
  def save(list: ListBuffer[CourseClickCount]): Unit = {
    val table = HBaseUtil.getInstance().getHTable(tableName)
    list.foreach(ccc => {
      table.incrementColumnValue(Bytes.toBytes(ccc.day_course), Bytes.toBytes(familyName), Bytes.toBytes(qualifier), ccc.clickcount)
    })
  }


  def get(day_course: String): Long = {
    val table = HBaseUtil.getInstance().getHTable(tableName)
    val get = new Get(Bytes.toBytes(day_course))
    val res = table.get(get).getValue(Bytes.toBytes(familyName), Bytes.toBytes(qualifier))
    Bytes.toLong(res)
  }

  def main(args: Array[String]): Unit = {
    val list: ListBuffer[CourseClickCount] = ListBuffer[CourseClickCount]()
    list.append(CourseClickCount("20171111_000", 5L))
    list.append(CourseClickCount("20171111_111", 8L))
    list.append(CourseClickCount("20171111_222", 10L))

    //    CourseClickCountDAO.save(list)
    print(CourseClickCountDAO.get("20171111_222"))
  }
}
