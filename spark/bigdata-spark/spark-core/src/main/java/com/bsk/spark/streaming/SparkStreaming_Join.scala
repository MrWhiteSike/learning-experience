package com.bsk.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming_Join {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(10))


    val line9 = ssc.socketTextStream("localhost", 9999)
    val line8 = ssc.socketTextStream("localhost", 8888)

    val map9 = line9.map((_, 1))
    val map8 = line8.map((_, 2))

    // DStream的join：其实就是两个RDD的join
    val join = map9.join(map8)
    join.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
