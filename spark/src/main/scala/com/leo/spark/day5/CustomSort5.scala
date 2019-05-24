package com.leo.spark.day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSort5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSort5").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val users= Array("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99")
    val lines: RDD[String] = sc.parallelize(users)

    val toRDD: RDD[(String, Int, Int)] = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toInt

      (name, age, fv)
    })

    val sorted: RDD[(String, Int, Int)] = toRDD.sortBy(tp => (-tp._3, tp._2))
    print(sorted.collect().toBuffer)

    sc.stop()
  }
}
