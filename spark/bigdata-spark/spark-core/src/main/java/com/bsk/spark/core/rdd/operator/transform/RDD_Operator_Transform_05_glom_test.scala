package com.bsk.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_05_glom_test {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))
    val rdd = sc.makeRDD(
      List(1, 2, 3, 4),2
    )

    val glomRDD = rdd.glom()
    val maxRDD = glomRDD.map(
      array => {
        array.max
      }
    )

    println(maxRDD.collect().sum)

    sc.stop()
  }

}
