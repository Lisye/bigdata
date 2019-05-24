package com.leo.spark.favteacher

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object GroupFavTeacher3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupFavTeacher3").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("/Users/leo/work/IdeaProject/learn/practice/hadoop/spark/src/main/resources/teacher.log")
    val subjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)
      val subject = new URL(httpHost).getHost.split("[.]")(0)

      ((subject, teacher), 1)
    })

    val reduced: RDD[((String, String), Int)] = subjectTeacherAndOne.reduceByKey(_+_)

    val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()

    val subjectPartirioner = new SubjectPartitoner(subjects)
    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(subjectPartirioner)

    val topN = 3
    val sorted: RDD[((String, String), Int)] = partitioned.mapPartitions(it => {
      it.toList.sortBy(_._2).reverse.take(topN).iterator
    })
    val result: Array[((String, String), Int)] = sorted.collect()

    println(result.toBuffer)

    sc.stop()
  }
}

class SubjectPartitoner(val sbs: Array[String]) extends Partitioner{

  val rules = new mutable.HashMap[String, Int]()
  var i = 0
  for (sb <- sbs){
    rules(sb) = i
//    rules.put(sb, i)
    i += 1
  }

  override def numPartitions: Int = sbs.length

  override def getPartition(key: Any): Int = {
    val subject = key.asInstanceOf[(String, String)]._1
    rules(subject)
  }
}