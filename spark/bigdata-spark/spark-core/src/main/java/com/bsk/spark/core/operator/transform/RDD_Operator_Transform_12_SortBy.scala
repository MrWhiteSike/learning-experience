package com.bsk.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_11_SortBy {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))
    val rdd = sc.makeRDD(
      List(4, 5, 1, 3, 2, 6),2
    )
    val sortRDD = rdd.sortBy(num => num)
    sortRDD.saveAsTextFile("output")

    sc.stop()
  }

}
