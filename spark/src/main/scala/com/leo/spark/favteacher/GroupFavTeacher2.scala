package com.leo.spark.favteacher

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupFavTeacher2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GroupFavTeacher2").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("/Users/leo/work/IdeaProject/learn/practice/hadoop/spark/src/main/resources/teacher.log")

    val topN = 3

    val subjectTeacherAndOn = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)
      val subject = new URL(httpHost).getHost.split("[.]")(0)

      ((subject, teacher), 1)
    })
    val reduced: RDD[((String, String), Int)] = subjectTeacherAndOn.reduceByKey(_+ _)


    val subjects = Array("bigdata", "javaee", "php")

    for (sb <- subjects) {
      val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1 == sb)
      val favTeacher: Array[((String, String), Int)] = filtered.sortBy(_._2, false).take(topN)

      println(favTeacher.toBuffer)

    }

    sc.stop()
  }
}
