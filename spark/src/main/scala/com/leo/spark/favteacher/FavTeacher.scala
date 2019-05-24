package com.leo.spark.favteacher

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavTeacher {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]") //启动4个线程
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("/Users/leo/work/IdeaProject/learn/practice/hadoop/spark/src/main/resources/teacher.log")

    val teachAndOne: RDD[(String, Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)

      (teacher, 1)
    })
    val reduced: RDD[(String, Int)] = teachAndOne.reduceByKey(_ + _)
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)

    val result: Array[(String, Int)] = sorted.collect()

    println(result.toBuffer)

    sc.stop()
  }
}
