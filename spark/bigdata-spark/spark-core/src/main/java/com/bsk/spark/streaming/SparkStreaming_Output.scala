package com.bsk.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming_Output {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    // reduceByKeyAndWindow(reduceFunc,invReduceFunc,windowLength,slideDuration)
    // 这种是有状态的保存，需要添加 checkpoint 的保存路径，否则抛出异常
    ssc.checkpoint("cp")

    val lines = ssc.socketTextStream("localhost", 9999)
    val wordToOne = lines.map((_, 1))

    val wordToCount = wordToOne.reduceByKeyAndWindow(
      (x:Int,y:Int) => {x + y},
      (x:Int,y:Int) => {x-y},
      Seconds(9),Seconds(3))

    // SparkStreaming 如果没有输出操作，那么会提示错误
//    wordToCount.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
