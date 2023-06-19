package com.bsk.networkflow_analysis

import java.text.SimpleDateFormat
import java.time.Duration

import com.bsk.bean.ApacheLogEvent
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.table.api._

object NetWorkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.readTextFile("C:\\Users\\Baisike\\opensource\\learning-experience\\flink\\UserBahaviorAnalysisScala\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")

    val dataStream = inputStream.map(data => {
      val dataArr = data.split(" ")
      val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val time = format.parse(dataArr(3)).getTime
      ApacheLogEvent(dataArr(0), dataArr(1), time, dataArr(5), dataArr(6))
    }).assignTimestampsAndWatermarks(WatermarkStrategy
      .forBoundedOutOfOrderness[ApacheLogEvent](Duration.ofMinutes(1))
      .withTimestampAssigner(new SerializableTimestampAssigner[ApacheLogEvent] {
        override def extractTimestamp(element: ApacheLogEvent, recordTimestamp: Long): Long = element.eventTime
      }))
//   dataStream
//     .filter(_.method == "GET")
//     .keyBy(_.url)
//      .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(5)))
//      .aggregate()



  }

}
