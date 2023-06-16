package com.bsk.hotitems

import com.bsk.bean.UserBehavior
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row

object HotItemsWithTable {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tEnv = StreamTableEnvironment.create(env)

    val inputDStream = env.readTextFile("C:\\Users\\Baisike\\opensource\\UserBahaviorAnalysisScala\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val dataDStream = inputDStream.map(data => {
      val dataArr = data.split(",")
      UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp*1000L)

    val table = tEnv.fromDataStream(dataDStream,
      Schema
        .newBuilder()
        .columnByExpression("ts", "TO_TIMESTAMP_LTZ(`timestamp`,3)")
        .watermark("ts", "ts - INTERVAL '10' SECOND")
        .build())
//    val table = tEnv.fromDataStream(dataDStream,
//        Schema.newBuilder()
//          .columnByMetadata("ts", "TIMESTAMP_LTZ(3)")
//          .watermark("ts", "SOURCE_WATERMARK()")
//          .build())


    table.printSchema()



    val table1 = table
      .filter($"behavior" === "pv")
      .window(Slide over 1.hour every 5.minutes on $"ts" as $"w")
      .groupBy($"w", $"itemId")
      .select(
        $"itemId",
        $"w".end as "endWindow",
        $"itemId".count as "cnt"
      )



    tEnv.createTemporaryView("table1", table1)

    val result = tEnv.sqlQuery(
      """
        |select *
        |from (
        |   select *,row_number() over(partition by endWindow order by cnt desc) as rn
        |   from table1
        |)
        |where rn <= 5
        |""".stripMargin)

    tEnv.toDataStream(table1, classOf[Row]).print("table1")
    tEnv.toDataStream(result, classOf[Row]).print("result")


    env.execute()

  }
}
