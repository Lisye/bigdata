package com.leo.spark.day7

import com.leo.spark.day4.MyUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object IpLocationSQL {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder().appName("IpLocationSQL").master("local[*]").getOrCreate()

		import spark.implicits._
		val rulesLines: Dataset[String] = spark.read.textFile("hdfs://hadoop102:9000/ip/ip.txt")
		val ruleDf: DataFrame = rulesLines.map(line => {
			val fields = line.split("[|]")
			val startNum = fields(2).toLong
			val endNum = fields(3).toLong
			val province = fields(6)

			(startNum, endNum, province)
		}).toDF("snum", "enum", "province")

		val accessLines: Dataset[String] = spark.read.textFile("hdfs://hadoop102:9000/ip/access.log")
		val ipDf: DataFrame = accessLines.map(line => {
			var fields = line.split("[|]")
			val ipNum = MyUtils.ip2Long(fields(1))

			ipNum
		}).toDF("ip_num")

		ruleDf.createTempView("v_rules")                       
		ipDf.createTempView("v_ips")

		val result: DataFrame = spark.sql("select province, count(*) counts from v_rules join v_ips on (ip_num >= snum and ip_num <= enum) group by province order by counts desc")
		result.show()

		spark.stop()
	}
}
