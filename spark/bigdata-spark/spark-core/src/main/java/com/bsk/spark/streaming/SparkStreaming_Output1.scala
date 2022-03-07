package com.bsk.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming_Output1 {

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

    /**
     * 注意：
     * 1、连接不能写在driver层面（序列化）
     * 2、如果写在foreach，则每个RDD中的每一条数据都创建，得不偿失
     * 3、增加foreachPartition ，在分区创建（获取）
     *
     *
     * */

    // foreachRDD 不会出现时间戳，底层使用RDD进行操作
//   wordToCount.foreachRDD(
////     rdd =>
//   )

    ssc.start()
    ssc.awaitTermination()
  }

}
