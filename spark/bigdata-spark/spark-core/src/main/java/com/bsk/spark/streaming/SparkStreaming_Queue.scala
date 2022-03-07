package com.bsk.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming_Queue {

  def main(args: Array[String]): Unit = {
    // TODO 创建环境对象

    // StreamingContext 创建时，需要传递两个参数
    // 第一个参数：表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数：表示批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val rddQueue = new mutable.Queue[RDD[Int]]()

    val inputStream = ssc.queueStream(rddQueue, false)
    val mapStream = inputStream.map((_, 1))
    val reduceStream = mapStream.reduceByKey(_ + _)
    reduceStream.print()

    // 1、启动采集器
    ssc.start()

    for (i <- 1 to 5){
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }


    // 2、等待采集器的关闭
    ssc.awaitTermination()
  }

}
