package com.bsk.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}


object SparkStreaming_Resume {

  def main(args: Array[String]): Unit = {

    // 从检查点中恢复数据，如果检查点中没有就重新创建
    val ssc = StreamingContext.getActiveOrCreate("cp", () => {
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
      val ssc = new StreamingContext(sparkConf, Seconds(3))
      val lines = ssc.socketTextStream("localhost", 9999)
      val wordToOne = lines.map((_, 1))
      wordToOne.print()
      ssc
    })

    // 设置检查点路径，保存检查点
    ssc.checkpoint("cp")

    // TODO 这可以启动一个线程去进行优雅的关闭操作

    ssc.start()
    ssc.awaitTermination() // block 阻塞main线程
  }

}
