package com.zjb.app.spark.project.domain

/**
  * 从搜索引擎引流过来的课程点击量实体类
  *
  * @param day_search_course
  * @param clickcount
  */
case class CourseSearchClickCount(day_search_course: String, clickcount: Long)
