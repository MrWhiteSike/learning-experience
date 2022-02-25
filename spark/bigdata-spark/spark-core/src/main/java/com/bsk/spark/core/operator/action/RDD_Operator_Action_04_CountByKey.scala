package com.bsk.spark.core.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Action_04_CountByKey {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)),2)

    // countByKey : 要求数据为 Key-Value 类型，统计数据源中 key 出现的个数（Long），返回一个Map，数据源中的 key 值作为Map的key，个数作为Map的value
    val stringToLong = rdd.countByKey()
    println(stringToLong)

    sc.stop()
  }

}
