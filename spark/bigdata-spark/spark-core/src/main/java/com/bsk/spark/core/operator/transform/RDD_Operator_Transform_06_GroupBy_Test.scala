package com.bsk.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_06_GroupBy_Test {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))
    val rdd = sc.makeRDD(
      List("Hello","Spark","Scala","Hadoop"),2
    )

    // 开头字符相同的字符串进行分组
    val groupRDD = rdd.groupBy(_.charAt(0))

    groupRDD.collect().foreach(println)


    sc.stop()
  }

}
