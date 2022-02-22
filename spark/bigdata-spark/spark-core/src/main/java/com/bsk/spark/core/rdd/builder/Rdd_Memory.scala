package com.bsk.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Rdd_Memory {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("CreateRDD"))
    // 从内存中创建RDD，将内存中集合的数据作为处理的数据源
    val seq = Seq[Int](elems = 1,2,3,4)
    // parallelize : 并行
    // val rdd = sc.parallelize(seq)

    val rdd = sc.makeRDD(seq)

    rdd.collect().foreach(println)
    sc.stop()
  }

}
