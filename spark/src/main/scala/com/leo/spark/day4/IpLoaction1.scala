package com.leo.spark.day4

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpLoaction1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IpLocation1").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val accessLines: RDD[String] = sc.textFile("hdfs://hadoop102:9000/ip/access.log")

    val rules: Array[(Long, Long, String)] = MyUtils.readRules("/Users/leo/work/IdeaProject/learn/practice/hadoop/spark/src/main/resources/ip.txt")

    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rules)

    val func = (line: String) => {
      val fields: Array[String] = line.split("[|]")
      val ip: String = fields(1)

      val ipNum: Long = MyUtils.ip2Long(ip)

      val rulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      val index: Int = MyUtils.binarySearch(rulesInExecutor, ipNum)

      var province = "unknow"
      if (index != -1) {
        province = rulesInExecutor(index)._3
      }

      (province, 1)
    }

    val provinceAndOne: RDD[(String, Int)] = accessLines.map(func)

    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey(_+_)

    val r: Array[(String, Int)] = reduced.collect()

    print(r.toBuffer)

    sc.stop()
  }
}
