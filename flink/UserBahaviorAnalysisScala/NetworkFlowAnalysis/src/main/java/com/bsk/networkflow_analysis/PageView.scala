package com.bsk.networkflow_analysis

import com.bsk.bean.UserBehavior
import com.bsk.function.{MyMapper, MyPvCountAgg, MyPvCountWindowResult, MyTotalPvCount}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val value = env.readTextFile("C:\\Users\\Baisike\\opensource\\learning-experience\\flink\\UserBahaviorAnalysisScala\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val dataStream = value.map(data => {
      val dataArr = data.split(",")
      UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)

    val resultStream = dataStream.filter(_.behavior == "pv")
//      .map(_ => ("pv", 1L)) // 转换为二元组，用一个key作为分组的key，容易造成数据倾斜问题
      .map(MyMapper(4)) // 解决只有一个key的数据倾斜问题
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.hours(1)))
      .aggregate(MyPvCountAgg(), MyPvCountWindowResult())
      .keyBy(_.windowEnd)
      .process(MyTotalPvCount())

    resultStream.print("data")
    env.execute("pv job")



  }
}
