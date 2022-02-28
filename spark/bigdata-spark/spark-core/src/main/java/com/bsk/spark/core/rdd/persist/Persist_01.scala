package com.bsk.spark.core.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}

object Persist_01 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Test")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map(
        word => {
          println(" #########")
          (word,1)
        }
      )
    val reduceRDD = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("*******************************")
    // RDD 中不存储数据
    // 如果一个RDD需要重复使用，那么需要从头再次执行来获取数据
    // RDD对象可以重用的，但是数据无法重用
    val groupRDD = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)


    sc.stop()
  }

}
