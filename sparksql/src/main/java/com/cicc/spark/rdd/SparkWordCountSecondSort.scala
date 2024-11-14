package com.cicc.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCountSecondSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkwordcountsort").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val line = sc.textFile("C:\\Users\\Baisike\\project\\sparksql\\src\\main\\resources\\text")

    // 转换为
    val mapRDD = line.map(f => {
      val strings = f.split(" ")
      (new SecondSortKey(strings(0), strings(1).toInt), s"${strings(0)}\t${strings(1)}")
    })

    val sortByKeyRDD = mapRDD.sortBy(_._1, false)

    sortByKeyRDD.take(100).foreach(f => println(f._2) )

    sc.stop()
  }
}
