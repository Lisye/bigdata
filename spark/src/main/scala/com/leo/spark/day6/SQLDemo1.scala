package com.leo.spark.day6

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

object SQLDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SQLDemo1").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    //先有一个普通的RDD，然后在关联上schema，进而转成DataFrame
    val lines = sc.textFile("hdfs://hadoop102:9000/person.txt")
    val boyRDD: RDD[Boy] = lines.map(line => {
      val fields: Array[String] = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble

      Boy(id, name, age, fv)
    })

    //将RDD转换成DataFrame
    //导入隐式转换
    import sqlContext.implicits._
    val bdf: DataFrame = boyRDD.toDF

    bdf.registerTempTable("t_boy")

    val result: DataFrame = sqlContext.sql("SELECT * FROM t_boy ORDER BY fv DESC, age asc")

    result.show()

    sc.stop()
  }
}

case class Boy(id: Long, name: String, age: Int, fv: Double)