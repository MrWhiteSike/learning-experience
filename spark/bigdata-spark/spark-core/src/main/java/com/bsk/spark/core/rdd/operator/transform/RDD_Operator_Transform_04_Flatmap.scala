package com.bsk.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_04_Flatmap {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))
    val rdd = sc.makeRDD(
      List(List(1, 2), List(3, 4))
    )

    val fmRDD = rdd.flatMap(
      list => {
        list
      }
    )

    fmRDD.collect().foreach(println)

    sc.stop()
  }

}
