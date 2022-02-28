package com.bsk.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object ACC_01 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Test")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(1,2,3,4))

    // reduce : 分区内计算，分区间计算
//    val i = rdd.reduce(_+_)
//    println(i)

    var sum = 0
    rdd.foreach(
      num => sum += num
    )

    println("sum = " + sum)







    sc.stop()
  }

}
