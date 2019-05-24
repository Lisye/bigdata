package com.leo.spark.day7

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object JoinTest {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder().appName("JoinTest").master("local[*]").getOrCreate()

		import spark.implicits._
		val lines: Dataset[String] = spark.createDataset(List("1,laozhao,china", "2,laoduan,usa", "3,laoyang,jp"))

		val tpDs: Dataset[(Long, String, String)] = lines.map(line => {
			val fields = line.split(",")
			val id = fields(0).toLong
			val name = fields(1)
			val nation = fields(2)

			(id, name, nation)
		})
		val df1: DataFrame = tpDs.toDF("id", "name", "nation")

		val nations: Dataset[String] = spark.createDataset(List("china,中国", "usa,美国"))

		val ndataset: Dataset[(String, String)] = nations.map(nation => {
			val fields = nation.split(",")
			val ename = fields(0)
			val cname = fields(1)

			(ename, cname)
		})
		val df2 = ndataset.toDF("ename", "cname")

		//spark sql join
		df1.createTempView("v_users")
		df2.createTempView("v_nations")
		val result: DataFrame = spark.sql("select id, name, nation, cname from v_users join v_nations on nation=ename")
		result.show()

		println("=" * 100)

		//dataset join
		val result1: DataFrame = df1.join(df2, $"nation" === $"ename", "left_outer")
		result1.show()


		spark.stop()
	}
}
