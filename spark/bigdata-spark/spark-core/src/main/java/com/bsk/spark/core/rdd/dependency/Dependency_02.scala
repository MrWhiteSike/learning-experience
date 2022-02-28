package com.bsk.spark.core.rdd.dependency

import org.apache.spark.{SparkConf, SparkContext}

object Dependency_02 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Test")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("datas/1.txt")
    println(lines.dependencies.mkString(","))
    println("************************")
    val words = lines.flatMap(_.split(" "))
    println(words.dependencies.mkString(","))
    println("************************")
    val wordGroup = words.groupBy(word => word)
    println(wordGroup.dependencies.mkString(","))
    println("************************")
    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    println(wordToCount.dependencies.mkString(","))
    println("************************")
    val array = wordToCount.collect()
    array.foreach(println)

    sc.stop()
  }

}
