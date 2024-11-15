package com.cicc.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}


//定义一个对象用于保存key对应的值和状态
case class StreamStateValue(var value:Int, var isUpdate:Boolean = false)
object SparkStreamingSocketPortUpdateStateObject {

  def main(args: Array[String]): Unit = {

    val checkpoint = "C:\\Users\\Baisike\\project\\sparksql\\src\\main\\resources\\output\\sparkstreamingsocketportupdatestateobject"
    val createFunction: () => StreamingContext = () => {

      val conf = new SparkConf().setAppName("sparkstreamingsocketportupdatestateobject").setMaster("local[*]")
      val streamingContext = new StreamingContext(conf, Durations.seconds(5))

      // 注意：使用 updateStateByKey 的时候，必须要设置一个checkpoint地址用于保存历史的数据
      streamingContext.checkpoint(checkpoint)

      val lines = streamingContext.socketTextStream("localhost", 6666)

      val flatMap = lines.flatMap(_.split(" "))

      // 在rdd之间传包含了值的 StreamStateValue 对象
      val mapToPair = flatMap.map((_, StreamStateValue(1)))
      val reduceByKey: DStream[(String, StreamStateValue)] = mapToPair.reduceByKey((a,b) => StreamStateValue(a.value + b.value))

      val updateStateByKey = reduceByKey.updateStateByKey((a: Seq[StreamStateValue], b: Option[StreamStateValue]) => {
        // 参数a：本批次的数据
        // 参数b：这个key上批次计算的历史结果
        var total = 0
        for (i <- a) {
          total += i.value
        }
        // 这里需要先判断一下，因为这个key有可能是第一次进入，也就是没有历史数据，那此时应该给个初始化值0
        val last = if (b.isDefined) b.get else StreamStateValue(0)

        // 如果本批次key 有更新的话，那这个key 对应 StreamStateValue 对象的状态修改为true
        if (a.size != 0){
          last.isUpdate = true
        }else{
          last.isUpdate = false
        }

        val now = total + last.value
        last.value = now

        // 使用这个修改了状态的 StreamStateValue 对象作为本批次这个key的结果
        Some(last)
      })

      // 在有checkpoint数据的时候，修改算子中的function是有效的
      // 使用了checkpoint恢复StreamingContext程序，这样的程序并不影响你后续升级代码
      updateStateByKey.foreachRDD((r, t) => {
        // 可以用rdd的isEmpty方法，判断此批次是否有数据，如果有数据再执行
        if (!r.isEmpty()){
          // 过滤状态为true的，代表本批次有更新的数据，这样的话比如插入数据库就不用全量插入，
          // 只插入没批次有变化的就可以了
          val filter = r.filter(_._2.isUpdate)
          println(s"count time : ${t}, ${r.collect().toList}")
          println(s"count time : ${t}, filter data : ${filter.collect().toList}")
        }
      })

      streamingContext
    }
    // 如何恢复历史数据？
    // 使用StreamingContext.getOrCreate 从checkpoint里恢复最后一次streamingcontext状态，
    // 如果没有checkpoint地址，那就新建一个 StreamingContext 对象
    val context = StreamingContext.getOrCreate(checkpoint, createFunction)

    // 这里必须得使用getOrCreate 判断好（要么是历史的，要么是新创建的）的那个 StreamingContext 进行启动
    context.start()
    context.awaitTermination()

  }
}
