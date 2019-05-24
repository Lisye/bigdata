package com.leo.spark.day4

import scala.io.{BufferedSource, Source}

object MyUtils {
  def main(args: Array[String]): Unit = {
    val ipNum: Long = ip2Long("114.215.43.42")
    val rules = readRules("/Users/leo/work/IdeaProject/learn/practice/hadoop/spark/src/main/resources/ip.txt")
    val index = binarySearch(rules, ipNum)

    println(index)
  }

  def ip2Long(ip: String): Long = {
    val fragments: Array[String] = ip.split("[.]")
    var ipNum = 0L
    for (i <- fragments.indices) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }

    ipNum
  }

  def readRules(path: String): Array[(Long, Long, String)] = {
    //读取ip规则
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    //对ip规则进行整理，并放入到内存
    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fileds = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    }).toArray
    rules
  }

  def binarySearch(lines: Array[(Long, Long, String)], ip: Long) : Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }
}