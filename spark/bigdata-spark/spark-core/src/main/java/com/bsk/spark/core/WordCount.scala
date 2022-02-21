package com.bsk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("WordCount"))
    val lines = sc.textFile("datas")
    val words = lines.flatMap(_.split(" "))
    val wordToOne = words.map(word => (word, 1))

    // 方法1：分组和聚合实现
//    val wordGroup = wordToOne.groupBy(t => t._1)
//    val wordToCount = wordGroup.map {
//      case (_, list) => {
//        list.reduce(
//          (t1, t2) => {
//            (t1._1, t1._2 + t2._2)
//          }
//        )
//      }
//    }

    // 方法2：Spark框架提供了更多的功能，可以将分组和聚合使用一个方法实现
    // reduceByKey : 相同的key的数据，可以对value进行reduce聚合操作
    val wordToCount = wordToOne.reduceByKey(_+_) // 匿名参数

    val array = wordToCount.collect()
    array.foreach(println)
    sc.stop()

  }

}
