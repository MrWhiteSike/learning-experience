package com.bsk.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Rdd_File {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Rdd_File"))
    // 从文件中创建RDD，将文件中集合的数据作为处理的数据源
    // path路径默认以当前环境的根路径为基准。可以写绝对路径，也可以写相对路径
//    val rdd = sc.textFile(path = "datas/1.txt")
    // path路径可以是文件的具体路径，也可以是目录名称
//    val rdd = sc.textFile(path = "datas")
    // path 路径还可以使用通配符 *
    // path 路径还可以是分布式存储系统路径：HDFS
    val rdd = sc.textFile(path = "datas/1*.txt")
    rdd.collect().foreach(println)
    sc.stop()
  }

}
