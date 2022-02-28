package com.bsk.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_13_DoubleValue1 {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))
    val rdd1 = sc.makeRDD(List(1,2,3,4,5,6), 2)
    val rdd2 = sc.makeRDD(List(3,4,5,6),2)

    // 分区数量不一致，报错信息：Can't zip RDDs with unequal numbers of partitions: List(4, 2)
    // 每个分区中数据数量不一致，报错信息：Can only zip RDDs with same number of elements in each partition
    val newRDD = rdd1.zip(rdd2)

    println(newRDD.collect().mkString(","))
    sc.stop()
  }

}
