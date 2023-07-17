package com.bsk.networkflow_analysis

import com.bsk.bean.UserBehavior
import com.bsk.function.{MyTrigger, MyUvCountWithBloom}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 独立用户访问数（对UserID进行去重统计）: 布隆过滤器实现
 */
object UvWithBloom {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val value = env.readTextFile("C:\\Users\\Baisike\\opensource\\learning-experience\\flink\\UserBahaviorAnalysisScala\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val dataStream = value.map(data => {
      val dataArr = data.split(",")
      UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)

    val resultStream = dataStream.filter(_.behavior == "pv")
        .map(data => ("uv", data.userId))
        .keyBy(_._1)
        .window(TumblingEventTimeWindows.of(Time.hours(1)))
        .trigger(MyTrigger())
        .process(MyUvCountWithBloom())
    resultStream.print("data")
    env.execute("uv job")



  }
}
