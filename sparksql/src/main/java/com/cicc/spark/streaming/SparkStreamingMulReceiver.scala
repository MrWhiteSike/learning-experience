package com.cicc.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

import scala.collection.mutable.ListBuffer

/**
 * 多receiver源 union 方式 合并数据流
 * 在一个stage里，没有进行shuffle
 *
 */
object SparkStreamingMulReceiver {

  def main(args: Array[String]): Unit = {
    // 这里设置 local[3] 为啥呢？ 因为这个程序里面有两个socket的receiver，
    // 每个receiver都占用了一个cpu， 所以设置的cpu必须大于2，
    // 不然就没有可以负责运行任务的cpu了
    val conf: SparkConf = new SparkConf().setAppName("sparkstreamingfilecogroup").setMaster("local[3]")
    val streamingContext = new StreamingContext(conf, Durations.seconds(5))

    // 生成两个socket流
    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 6666)
    val lines1: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 6667)

    // 合并两个socket流
    val buffer = new ListBuffer[DStream[String]]
    buffer += lines
    buffer += lines1
    val unionDStream: DStream[String] = streamingContext.union(buffer)

    // 合并了的两个流使用了统一的业务处理逻辑
    unionDStream.map((_,1)).foreachRDD(r =>{
      r foreach println
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
