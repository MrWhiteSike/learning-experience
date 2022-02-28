package com.bsk.spark.core.rdd.dependency

import org.apache.spark.{SparkConf, SparkContext}

object Dependency_01 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Test")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("datas/1.txt")
    println(lines.toDebugString)
    println("************************")
    val words = lines.flatMap(_.split(" "))
    println(words.toDebugString)
    println("************************")
    val wordGroup = words.groupBy(word => word)
    println(wordGroup.toDebugString)
    println("************************")
    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    println(wordToCount.toDebugString)
    println("************************")
    val array = wordToCount.collect()
    array.foreach(println)

    sc.stop()
  }

}
