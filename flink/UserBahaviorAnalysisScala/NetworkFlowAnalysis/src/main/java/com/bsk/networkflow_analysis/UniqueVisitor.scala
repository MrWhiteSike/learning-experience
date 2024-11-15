package com.bsk.networkflow_analysis

import com.bsk.bean.UserBehavior
import com.bsk.function.{MyMapper, MyPvCountAgg, MyPvCountWindowResult, MyTotalPvCount, MyUvCountResult}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 独立用户访问数（对UserID进行去重统计）
 */
object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val value = env.readTextFile("C:\\Users\\Baisike\\opensource\\learning-experience\\flink\\UserBahaviorAnalysisScala\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val dataStream = value.map(data => {
      val dataArr = data.split(",")
      UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)

    val resultStream = dataStream.filter(_.behavior == "pv")
        // 进行开窗统计聚合，全窗口函数统计每小时的UV值
        .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
        .apply(MyUvCountResult())

    resultStream.print("data")
    env.execute("uv job")



  }
}
