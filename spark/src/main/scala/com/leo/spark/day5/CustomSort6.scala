package com.leo.spark.day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSort6 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSort6").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val users= Array("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99")

    val lines: RDD[String] = sc.parallelize(users)
    val tpRDD: RDD[(String, Int, Int)] = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toInt

      (name, age, fv)
    })

    implicit val rules = Ordering[(Int, Int)].on[(String, Int, Int)](t => (-t._3, t._2))
    val sorted: RDD[(String, Int, Int)] = tpRDD.sortBy(tp => tp)
    print(sorted.collect().toBuffer)

    sc.stop()
  }
}
