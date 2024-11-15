package com.cicc.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingSocketPortUpdateState {


  def main(args: Array[String]): Unit = {

    val checkpoint = "C:\\Users\\Baisike\\project\\sparksql\\src\\main\\resources\\output\\sparkstreamingsocketportupdatestate"

      val conf = new SparkConf().setAppName("sparkstreamingsocketportupdatestate").setMaster("local[*]")
      val streamingContext = new StreamingContext(conf, Durations.seconds(5))

      // 注意：使用 updateStateByKey 的时候，必须要设置一个checkpoint地址用于保存历史的数据
      streamingContext.checkpoint(checkpoint)

      // 注意：在有checkpoint数据的时候， 修改地址和端口是不生效的
      val lines = streamingContext.socketTextStream("localhost", 6666)
      // 测试修改地址和端口是否生效
      //      val lines = streamingContext.socketTextStream("localhost", 7777)

      val flatMap = lines.flatMap(_.split(" "))
      val mapToPair = flatMap.map((_, 1))
      val reduceByKey: DStream[(String, Int)] = mapToPair.reduceByKey(_ + _)

      val updateStateByKey = reduceByKey.updateStateByKey((a: Seq[Int], b: Option[Int]) => {
        // 参数a：本批次的数据
        // 参数b：这个key上批次计算的历史结果
        var total = 0
        for (i <- a) {
          total += i
        }
        // 这里需要先判断一下，因为这个key有可能是第一次进入，也就是没有历史数据，那此时应该给个初始化值0
        val last = if (b.isDefined) b.get else 0

        // 测试修改是否生效
        println(last)
        val now = total + last
        Some(now)
      })

      // 在有checkpoint数据的时候，修改算子中的function是有效的
      // 使用了checkpoint恢复StreamingContext程序，这样的程序并不影响你后续升级代码
      updateStateByKey.foreachRDD((r, t) => {
        // 测试修改是否生效
        //        println(s"count time : ${t}, ${r.collect().toList}")
        println(s"count time : ${t}, ${r.collect().toList}， more")
      })

    // 这里必须得使用getOrCreate 判断好（要么是历史的，要么是新创建的）的那个 StreamingContext 进行启动
    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
