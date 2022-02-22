package com.bsk.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_04_Flatmap1 {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))
    val rdd = sc.makeRDD(
      List(List(1, 2),3, List(3, 4))
    )

//    val fmRDD = rdd.flatMap(
//      data => {
//        // 采用模式匹配实现
//        data match {
//          case list: List[_] => list
//          case d => List(d)
//        }
//      }
//    )

    // 模式匹配的简写版
    val fmRDD = rdd.flatMap {
      case list: List[_] => list
      case d => List(d)
    }


    fmRDD.collect().foreach(println)

    sc.stop()
  }

}
