package com.bsk.networkflow_analysis

import java.text.SimpleDateFormat

import com.bsk.bean.ApacheLogEvent
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 实时流量统计 -- 热门页面浏览量TopN
 *
 * 基本需求：
 *  从web服务器的日志中，统计实时的热门访问页面
 *  统计每分钟的IP访问量，取出访问量最大的5个IP地址，每5秒更新一次
 *
 * 解决思路：
 *  将服务器日志中的时间，转换为时间戳，作为Event Time
 *  构建滑动窗口，窗口长度为1分钟，滑动距离为5秒
 *
 *
 */
object NetWorkFlowWithTable {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tEnv = StreamTableEnvironment.create(env)

    val inputDStream = env.readTextFile("C:\\Users\\Baisike\\opensource\\learning-experience\\flink\\UserBahaviorAnalysisScala\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")

    val dataDStream = inputDStream.map(data => {
      val dataArr = data.split(" ")
      val simpleDataFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val timestamp = simpleDataFormat.parse(dataArr(3)).getTime
      ApacheLogEvent(dataArr(0), dataArr(1), timestamp , dataArr(5), dataArr(6))
    }).assignAscendingTimestamps(_.eventTime * 1000L)

    val table = tEnv.fromDataStream(dataDStream,
      Schema
        .newBuilder()
        .columnByExpression("ts", "TO_TIMESTAMP_LTZ(eventTime,3)")
        .watermark("ts", "ts - INTERVAL '10' SECOND")
        .build()
    )

    table.printSchema()

    val table1 = table
        .filter($"method" === "GET")
        .window(Slide over 1.minute every 5.second() on $"ts" as $"w")
        .groupBy($"w", $"url")
        .select(
          $"url",
          $"w".end as "tend",
          $"url".count() as "cnt"
        ).addColumns($"tend".cast(DataTypes.TIMESTAMP_LTZ(3)).as("windowEnd"))

    table1.printSchema()

    tEnv.createTemporaryView("agg", table1)
    val result = tEnv.sqlQuery(
      """
        |select *
        |from (
        |     select url,windowEnd,cnt,row_number() over(partition by windowEnd order by cnt desc) rn
        |     from agg
        |)
        |where rn <= 5
        |""".stripMargin)
    tEnv.toChangelogStream(result).print("result")
    env.execute()
  }

}
