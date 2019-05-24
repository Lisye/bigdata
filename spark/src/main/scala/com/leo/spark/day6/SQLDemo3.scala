package com.leo.spark.day6

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object SQLDemo3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SQLDemo3").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val lines: RDD[String] = sc.textFile("hdfs://hadoop102:9000/person.txt")
    val rowRDD: RDD[Row] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble

      Row(id, name, age, fv)
    })
    rowRDD

    val sch: StructType = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("fv", DoubleType, true)
    ))

    val bdf: DataFrame = sqlContext.createDataFrame(rowRDD, sch)

    val df1: DataFrame = bdf.select("name", "age", "fv")

    import sqlContext.implicits._
    val df2: DataFrame = df1.orderBy($"fv" desc, $"age" asc)

    df2.show()

    sc.stop()
  }
}
