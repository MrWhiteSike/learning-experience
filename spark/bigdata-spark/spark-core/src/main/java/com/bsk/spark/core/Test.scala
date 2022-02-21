package com.bsk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Test {

  def main(args: Array[String]): Unit = {
//    println("hello spark")

    // TODO 建立和Spark框架的连接
    val conf = new SparkConf().setMaster("local").setAppName("Test")
    val sc = new SparkContext(conf)
    // TODO 执行业务操作
    // 1、读取文件，按行读取
    val lines = sc.textFile("datas")
    // 2、行解析，将行数据进行拆分
    val words = lines.flatMap(_.split(" "))
    // 3、将数据根据单词进行分组，便于统计
    val wordGroup = words.groupBy(word => word)
    // 4、对分组后的数据进行转换
    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    // 5、将数据结果采集到控制台打印
    val array = wordToCount.collect()
    array.foreach(println)



    // TODO 关闭连接
    sc.stop()

  }

}
