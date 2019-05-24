package com.leo.spark.day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSort3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSort3").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val users = Array("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99")
    val lines: RDD[String] = sc.parallelize(users)
    val userRDD: RDD[(String, Int, Int)] = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toInt

      (name, age, fv)
    })
//    val sorted: RDD[(String, Int, Int)] = userRDD.sortBy(tp => new Man1(tp._2, tp._3))
//    print(sorted.collect().toBuffer)

    sc.stop()
  }
}

case class Man1(age: Int, fv: Int) extends Ordered[Man] {
  override def compare(that: Man): Int = {
    if (this.fv == that.fv) {
      this.age - that.age
    } else {
      -(this.fv - that.fv)
    }
  }
}