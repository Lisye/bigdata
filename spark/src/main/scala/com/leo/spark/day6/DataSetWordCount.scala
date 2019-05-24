package com.leo.spark.day6

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object DataSetWordCount {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder().appName("DataSetWordCount").master("local[*]").getOrCreate()
		val lines: Dataset[String] = spark.read.textFile("hdfs://hadoop102:9000/spark/words.txt")

		import spark.implicits._
		val words: Dataset[String] = lines.flatMap(_.split(" "))

		//使用DataSet的API（DSL）
//		val result: Dataset[Row] = words.groupBy($"value" as "word").count().sort()
//		result.show()

		//导入聚合函数   agg()中传入聚合函数
		import org.apache.spark.sql.functions._
		val counts: Dataset[Row] = words.groupBy($"value".as("word")).agg(count("*") as "counts").orderBy($"counts" desc)
		counts.show()

		spark.stop()
	}
}
