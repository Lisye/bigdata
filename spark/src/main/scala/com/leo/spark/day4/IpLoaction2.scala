package com.leo.spark.day4

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpLoaction2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("IpLoaction2").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rulesLines: RDD[String] = sc.textFile("hdfs://hadoop102:9000/ip/ip.txt")

    val ipRulesRdd: RDD[(Long, Long, String)] = rulesLines.map(line => {
      val fields: Array[String] = line.split("[|]")
      val startNum: Long = fields(2).toLong
      val endNum: Long = fields(3).toLong
      val province: String = fields(6)

      (startNum, endNum, province)
    })
    val rulesInDriver: Array[(Long, Long, String)] = ipRulesRdd.collect()

    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rulesInDriver)

    val rulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value

    val accessLines: RDD[String] = sc.textFile("hdfs://hadoop102:9000/ip/access.log")

    val provinceAndOne: RDD[(String, Int)] = accessLines.map(line => {
      val fields: Array[String] = line.split("[|]")
      val ip: String = fields(1)
      val ipNum = MyUtils.ip2Long(ip)

      val index: Int = MyUtils.binarySearch(rulesInExecutor, ipNum)

      var province = "unknow"
      if (index != -1) {
        province = rulesInExecutor(index)._3
      }

      (province, 1)
    })
    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey(_+_)
    val result: Array[(String, Int)] = reduced.collect()

    print(result.toBuffer)
  }
}
