package com.bsk.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_16_GroupByKey {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))

    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",2)))

    // groupByKey : 将数据源中的数据，相同的key的数据分到一个组中，形成一个对偶元组
    //                元组中的第一个元素就是key，第二个元素就是相同key的value集合
    val groupRDD = rdd.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop()
  }

}
