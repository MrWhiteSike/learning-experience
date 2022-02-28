package com.bsk.spark.core.rdd.IO

import org.apache.spark.{SparkConf, SparkContext}

object RDD_IO_Load {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Test")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("output")
    println(rdd.collect().mkString(","))
    val rdd1 = sc.objectFile[(String, Int)]("output1")
    println(rdd1.collect().mkString(","))
    val rdd2 = sc.sequenceFile[String, Int]("output2")
    println(rdd2.collect().mkString(","))

    sc.stop()
  }

}
