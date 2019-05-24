package com.leo.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object sql {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sql").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("hdfs://hadoop102:9000/sql")
    val filtered: RDD[String] = lines.filter(line => {
      !line.startsWith("INSERT INTO")
    })

    filtered.saveAsTextFile("hdfs://hadoop102:9000/sqlout/sql.txt")

//    val result: Array[String] = filtered.collect()
//
//    val buffer: mutable.Buffer[String] = result.toBuffer
//
//    for (line <- buffer) {
//      println(line)
//    }

    sc.stop()
  }
}
