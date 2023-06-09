package com.bsk.hotitems

import com.bsk.bean.{ItemViewCount, UserBehavior}
import com.bsk.function.{MyCountAggFunction, MyTopNHotItems, MyWindowResultFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
// 需要导入scala隐式转换， 不然会报错：No implicit arguments of type:Typelnformationt
import org.apache.flink.streaming.api.scala._

/**
 * 热门商品统计
 */
object HotItems {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) // 设置并行度为1

    // 从文件读取数据
    val inputDStream = env.readTextFile("C:\\Users\\Baisike\\opensource\\UserBahaviorAnalysisScala\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")

    // 转换成样例类
    val dataDStream = inputDStream.map(
      data => {
        val dataArr = data.split(",")
        UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
      }
    ).assignAscendingTimestamps(_.timestamp * 1000L)

    // 进行开窗聚合转换
    val aggDStream: DataStream[ItemViewCount] = dataDStream
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
      .aggregate(MyCountAggFunction(),MyWindowResultFunction())

    // 对统计聚合结果按照窗口分组，排序输出
    val resultDStream = aggDStream.keyBy(_.windowEnd)
      .process(MyTopNHotItems(5))

    // 测试打印输出
    resultDStream.print()

    env.execute("hot items job")
  }

}
