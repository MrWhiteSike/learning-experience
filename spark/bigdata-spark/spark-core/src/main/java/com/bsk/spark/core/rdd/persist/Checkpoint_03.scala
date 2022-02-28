package com.bsk.spark.core.rdd.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Checkpoint_03 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Test")
    val sc = new SparkContext(conf)
    // 设置 checkpoint 路径
    sc.setCheckpointDir("cp")

    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map(
        word => {
          println(" #########")
          (word,1)
        }
      )
    // checkpoint 需要落盘，那就需要指定检查点保存路径
    // 检查点路径中保存文件，当作业执行完成时，不会被删除
    // 一般保存路径都是在分布式存储系统 ：HDFS

    // 为了能够提高效率，一般情况下，是需要我们和cache联合使用的
    mapRDD.cache()
    mapRDD.checkpoint()
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
