package com.bsk.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_Part {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))
    val rdd = sc.makeRDD(
      List(1, 2, 3, 4),2
    )
    // 转换操作之后分区不变

    // 【1，2】【3，4】
    rdd.saveAsTextFile("output")

    // 【2，4】【6，8】
    val mapRDD = rdd.map(_*2)
    mapRDD.saveAsTextFile("output1")

    sc.stop()
  }

}
