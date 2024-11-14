package com.cicc.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCountSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkwordcountsort").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 用代码设置这个Application的日志级别
//    sc.setLogLevel("debug")

    val line = sc.textFile("C:\\Users\\Baisike\\project\\sparksql\\src\\main\\resources\\text")

    // spark 中的sort是个全局排序，因为其自己实现了一个RangePartitioner，而mr想全局排序必须自己实现一个Partitioner
    val sort = line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).sortBy(_._2)

    sort.saveAsTextFile("C:\\Users\\Baisike\\project\\sparksql\\src\\main\\resources\\output\\wordcount")

    // 打印rdd的debug信息可以方便的查看rdd的依赖，从而可以看到哪一步产量了shuffle
    println(sort.toDebugString)

    // 控制sparkcontext的退出
    sc.stop()
  }
}
