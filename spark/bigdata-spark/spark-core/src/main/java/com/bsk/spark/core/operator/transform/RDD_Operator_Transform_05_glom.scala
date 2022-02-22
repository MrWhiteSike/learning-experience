package com.bsk.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_05_glom {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))
    val rdd = sc.makeRDD(
      List(1, 2, 3, 4),2
    )

    val gRDD = rdd.glom()

    gRDD.collect().foreach(data => println(data.mkString(",")))


    sc.stop()
  }

}
