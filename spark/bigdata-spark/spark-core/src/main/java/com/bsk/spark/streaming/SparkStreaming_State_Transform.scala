package com.bsk.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming_State_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))


    val lines = ssc.socketTextStream("localhost", 9999)

    // transform 方法可以将底层的RDD，获取后进行操作，使用场景：
    // 1、DStream 功能不完善
    // 2、需要代码周期性的执行

    // 两种方式的区别：
    // code：Driver 端
    lines.transform(
      rdd => {
        // code：Driver 端 （周期性执行）
        rdd.map(
          str => {
            // code：Executor端
            str
          }
        )
      }
    )

    // code：Driver 端
    lines.map(
      data => {
        // code：Executor端
        data
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

}
