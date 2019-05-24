package com.leo.spark.favteacher

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupFavTeacher1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupFavTeacher1").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("/Users/leo/work/IdeaProject/learn/practice/hadoop/spark/src/main/resources/teacher.log")

    //    http://bigdata.edu360.cn/laoduan
    val sbjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)

      val subject = new URL(httpHost).getHost.split("[.]")(0)

      ((subject, teacher), 1)
    })

    val reduced: RDD[((String, String), Int)] = sbjectTeacherAndOne.reduceByKey(_+_)

    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)

    val sorted: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(3))

    val result: Array[(String, List[((String, String), Int)])] = sorted.collect()

    println(result.toBuffer)

    sc.stop()

  }
}
