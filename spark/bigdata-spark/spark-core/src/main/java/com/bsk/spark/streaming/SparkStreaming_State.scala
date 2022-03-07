package com.bsk.spark.streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming_State {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // requirement failed: The checkpoint directory has not been set
    // 说明：在使用有状态操作时，需要设定检查点路径
    ssc.checkpoint("cp")


    // 无状态数据操作，只对当前的采集周期内的数据进行处理
    // 在某些场合下，需要保留数据统计结果（状态），实现数据的汇总
    val data = ssc.socketTextStream("localhost", 9999)

    val wordToOne = data.map((_, 1))

//    val wordToCount = wordToOne.reduceByKey(_ + _)
    // updateStateByKey：根据key对数据的状态进行更新
    // 传递两个参数：
    // 第一个：表示相同的key的value 的集合
    // 第二个：表示缓存区相同key的value数据
    val wordToCount = wordToOne.updateStateByKey(
      (seq: Seq[Int], buff: Option[Int]) => {
        // 就是把缓存区的数据（可有可无的）取出，和当前区间的数据进行聚合，然后将聚合的值更新到缓存区中
        val newCount = buff.getOrElse(0) + seq.sum
        Option(newCount)
      }
    )


    wordToCount.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
