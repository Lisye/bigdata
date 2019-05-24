package com.leo.spark.day6

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SQLWordCount {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder().appName("SQLWordCount").master("local[*]").getOrCreate()
		val lines: Dataset[String] = spark.read.textFile("hdfs://hadoop102:9000/spark/words.txt")

		import spark.implicits._
		val words: Dataset[String] = lines.flatMap(_.split(" "))

		words.createTempView("v_wc")
		val result: DataFrame = spark.sql("select value word, count(*) counts from v_wc group by word order by counts desc")

		result.show()

		spark.stop()
	}
}
