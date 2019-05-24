package com.leo.spark.day6

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SQLTest1 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("SQLTest1").master("local[*]").getOrCreate()

    val lines: RDD[String] = session.sparkContext.textFile("hdfs://hadoop102:9000/person.txt")
	  val rowRDD: RDD[Row] = lines.map(line => {
		  val fields = line.split(",")
		  val id = fields(0).toLong
		  val name = fields(1)
		  val age = fields(2).toInt
		  val fv = fields(3).toDouble

		  Row(id, name, age, fv)
	  })

	  val schema: StructType = StructType(List(
		  StructField("id", LongType, true),
		  StructField("name", StringType, true),
		  StructField("age", IntegerType, true),
		  StructField("fv", DoubleType, true)
	  ))

	  val df: DataFrame = session.createDataFrame(rowRDD, schema)

	  import session.implicits._
	  val df2: Dataset[Row] = df.where($"fv" > 98).orderBy($"fv" desc, $"age" asc)

	  df2.show()

	  session.stop()
  }
}
