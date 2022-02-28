package com.bsk.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Action_03_CountByValue {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4),2)

    // countByValue ： 统计数据源中value出现的个数（Long），返回一个Map，value值作为Map的key，个数作为Map的value
    val intToLong = rdd.countByValue()

    println(intToLong)

    sc.stop()
  }

}
