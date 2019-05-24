package com.leo.spark.day5

object SortRules {

  implicit object OrderingXiaoRou extends Ordering[XianRou] {
    override def compare(x: XianRou, y: XianRou): Int = {
      if (x.fv == y.fv) {
        x.age - y.age
      } else {
        -(x.fv - y.fv)
      }
    }
  }

}
