package com.bsk.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Rdd_File1 {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Rdd_File"))
    // 从文件中创建RDD，将文件中集合的数据作为处理的数据源
    // textFile : 以行为单位来读取数据
    // wholeTextFiles ： 以文件为单位读取数据
    //    读取的结果表示为元组，第一个参数表示文件路径，第二个参数为文件内容
    val rdd = sc.wholeTextFiles(path = "datas/1*")
    rdd.collect().foreach(println)
    sc.stop()
  }

}
