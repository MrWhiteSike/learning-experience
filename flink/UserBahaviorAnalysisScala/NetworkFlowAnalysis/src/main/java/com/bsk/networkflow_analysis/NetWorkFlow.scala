package com.bsk.networkflow_analysis

import java.text.SimpleDateFormat
import java.time.Duration

import com.bsk.bean.ApacheLogEvent
import com.bsk.function.{MyPageCountAgg, MyPageCountWindowResult, MyTopNHotPage}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object NetWorkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


//    val inputStream = env.readTextFile("C:\\Users\\Baisike\\opensource\\learning-experience\\flink\\UserBahaviorAnalysisScala\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
    val inputStream = env.socketTextStream("localhost",8888)


    /**
     * Flink 处理实时数据的三重保障
     *
     * 1. window + watermark 来处理乱序数据
     * 2. allowedLateness 处理迟到数据 ，相当于延迟了window的生命周期
     * 3. sideOutput 最后的兜底策略，当window的生命周期结束后，延迟的数据可以通过侧输出流收集起来
     * 自定义后续的处理流程
     *
     * 对于乱序数据：
     * window + watermark 机制：处理乱序数据，本质是延缓一段时间进行窗口数据的计算
     *
     * 对于迟到数据：
     * allowedLateness：处理迟到一定时间内的数据，每来一条对应窗口计算一次
     *
     * 对于watermark + 迟到时间 之后的数据：
     * sideOutputLateData：输出到侧输出流 OutputTag
     *
     *
     */
    val dataStream = inputStream.map(data => {
      val dataArr = data.split(" ")
      val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val time = format.parse(dataArr(3)).getTime
      ApacheLogEvent(dataArr(0), dataArr(1), time, dataArr(5), dataArr(6))
    }).assignTimestampsAndWatermarks(WatermarkStrategy
      .forBoundedOutOfOrderness[ApacheLogEvent](Duration.ofSeconds(1))
      .withTimestampAssigner(new SerializableTimestampAssigner[ApacheLogEvent] {
        override def extractTimestamp(element: ApacheLogEvent, recordTimestamp: Long): Long = element.eventTime
      }))

    val outputTag = OutputTag[ApacheLogEvent]("late")

    // 注意：想要得到侧输出流，agg 和 result 需要分开写，否则得不到侧输出流的数据
    val agg = dataStream
      .filter(_.method == "GET")
      .keyBy(_.url)
      .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(5)))
      // 处理迟到数据:
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(outputTag)
      .aggregate(MyPageCountAgg(), MyPageCountWindowResult())

      val result = agg.keyBy(_.windowEnd)
      .process(MyTopNHotPage(5))

    val late = agg.getSideOutput(outputTag)
    agg.print("agg>>")
    late.print("late>>")
    result.print("result>>")

    env.execute("network flow job")
  }

}
