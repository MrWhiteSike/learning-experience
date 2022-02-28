package com.bsk.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_13_DoubleValue {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))
    val rdd1 = sc.makeRDD(List(1,2,3,4))
    val rdd2 = sc.makeRDD(List(3,4,5,6))

    // 要求两个数据源数据类型保持一致
    // 求交集
//    val newRDD = rdd1.intersection(rdd2)
    // 求并集 : 只是合并不去重，要想去重可以使用 distinct 算子进行去重
    val newRDD = rdd1.union(rdd2)
    // 求差集
//    val newRDD = rdd1.subtract(rdd2)
    // 拉链, 对应位置一对一映射，组成（key,value）
//    val newRDD = rdd1.zip(rdd2)

    println(newRDD.collect().mkString(","))
    sc.stop()
  }

}
