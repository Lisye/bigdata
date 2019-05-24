package com.leo.spark.favteacher

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object GroupFavTeacher4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GroupFavTeacher4").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("/Users/leo/work/IdeaProject/learn/practice/hadoop/spark/src/main/resources/teacher.log")


    val subjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)
      val subject = new URL(httpHost).getHost.split("[.]")(0)

      ((subject, teacher), 1)
    })
    subjectTeacherAndOne

    val subjects: Array[String] = subjectTeacherAndOne.map(_._1._1).distinct().collect()

    val sbPartitioner = new SubjectPartitioner2(subjects)

    val reduced: RDD[((String, String), Int)] = subjectTeacherAndOne.reduceByKey(sbPartitioner, _ + _)

    val topN = 3
    val sorted: RDD[((String, String), Int)] = reduced.mapPartitions(it => {
      it.toList.sortBy(_._2).take(topN).iterator
    })
    val result = sorted.collect()

    println(result.toBuffer)
  }
}

class SubjectPartitioner2(val sbs: Array[String]) extends Partitioner() {

  val rules = new mutable.HashMap[String, Int]()
  var i = 0
  for (sb <- sbs) {
    rules(sb) = i
    i += 1
  }

  override def numPartitions: Int = sbs.length

  override def getPartition(key: Any): Int = {
    val subject = key.asInstanceOf[(String, String)]._1
    rules(subject)
  }
}
