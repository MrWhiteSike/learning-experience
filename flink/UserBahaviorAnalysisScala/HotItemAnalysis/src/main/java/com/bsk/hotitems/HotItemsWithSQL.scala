package com.bsk.hotitems

import com.bsk.bean.UserBehavior
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row



// 导入隐式转换: stream & table
// imports for Scala DataStream API
//import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._

// imports for Table API with bridging to Scala DataStream API
import org.apache.flink.table.api._
//import org.apache.flink.table.api.bridge.scala._

object HotItemsWithSQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    val inputDStream = env.readTextFile("C:\\Users\\Baisike\\opensource\\UserBahaviorAnalysisScala\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val dataDStream = inputDStream.map(
      data => {
        val dataArr = data.split(",")
        UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
      }
    ).assignAscendingTimestamps(_.timestamp * 1000L)

//    dataDStream.print()

    // 方式1：fromDataStream
    // 1.1 不添加时间属性
//    val table = tableEnv.fromDataStream(dataDStream)
//    val table = tableEnv.fromDataStream(dataDStream).as("userId", "itemId", "categoryId", "behavior", "timestamp")

    // 1.2 使用Schema，可以添加时间属性

    // 处理时间
//    val table = tableEnv.fromDataStream(dataDStream,
//      Schema.newBuilder()
//    .columnByExpression("pt", "PROCTIME()")
//    .build())

    // 事件时间 : 时间列 + watermark 才可以生成 时间属性列
//    val table = tableEnv.fromDataStream(dataDStream,
//      Schema.newBuilder()
//        .columnByExpression("rt", "CAST(`timestamp` AS TIMESTAMP_LTZ(3))")
//        .watermark("rt", "rt - INTERVAL '10' SECOND")
//        .build())
//    val table = tableEnv.fromDataStream(dataDStream,
//      Schema.newBuilder()
//        .columnByMetadata("rt", "TIMESTAMP_LTZ(3)")
//        .watermark("rt", "SOURCE_WATERMARK()")
//        .build())

    val table = tableEnv.fromDataStream(dataDStream,
      Schema.newBuilder()
        .column("timestamp", "TIMESTAMP_LTZ(3)")
        .column("userId", "BIGINT")
        .column("itemId", "BIGINT")
        .column("categoryId", "INT")
        .column("behavior", "STRING")
        .watermark("timestamp", "SOURCE_WATERMARK()")
        .build())


    // 1.3 添加时间属性的fromDataStream(过时的)
//    val table = tableEnv.fromDataStream(dataDStream,'userId, 'itemId, 'categoryId, 'behavior, 'timestamp.rowtime as 'ts)
//    table.printSchema()
//    tableEnv.createTemporaryView("hotItem", table)


    // 方式2：createTemporaryView 直接创建临时表
    // 2.1 不使用Schema，不添加时间属性
//    tableEnv.createTemporaryView("hotItem", dataDStream)

    // 2.2 使用Schema，可以定义时间属性
    // 处理时间、事件时间 Schema的使用方式相同
    tableEnv.createTemporaryView("hotItem",
      dataDStream,
      Schema
        .newBuilder()
        .columnByExpression("ts", "TO_TIMESTAMP_LTZ(`timestamp`,3)")
        .watermark("ts", "ts - INTERVAL '10' SECOND")
        .build())

    // 2.3 表达式 (过时了)
//    tableEnv.createTemporaryView("hotItem", dataDStream, 'userId, 'itemId, 'categoryId, 'behavior, 'timestamp.rowtime as 'ts)

    // 测试
//    val table1 = tableEnv.sqlQuery("select * from hotItem")
//    table1.printSchema()



    /**
     * 抛出异常：org.apache.flink.table.api.TableException:
     * Table sink '*anonymous_datastream_sink$2*' doesn't support consuming update and delete changes
     * which is produced by node Rank(strategy=[UndefinedStrategy], rankType=[ROW_NUMBER],
     * rankRange=[rankStart=1, rankEnd=5], partitionBy=[$2], orderBy=[cnt DESC], select=[itemId, cnt, $2, rn])
     *
     * 原因：你用了普通的group by的话，那么它的结果就是有更新的，所以需要sink支持写入update的结果，
     *
     * 解决：
     * 方法一
     * 使用toChangelogStream 生成update流进行输出
     *
     * 方法二 （推荐）
     * 使用 Window TVF（表值函数）替换 Window 聚合函数，生成 append 流
     *
     */

      // 方法一
    //    val resultTable = tableEnv.sqlQuery(
    //      """
    //        |select *
    //        |from (
    //        |   select *,row_number() over(partition by windowEnd order by cnt desc) as rn
    //        |   from (
    //        |       select itemId,hop_end(ts,interval '5' minute, interval '1' hour) as windowEnd,count(itemId) as cnt
    //        |       from hotItem
    //        |       where behavior = 'pv'
    //        |       group by itemId,hop(ts,interval '5' minute, interval '1' hour)
    //        |       )
    //        |   )
    //        |where rn <= 5
    //        |""".stripMargin)

    //    tableEnv.toChangelogStream(resultTable).print()

      // 方法二实现
    val resultTable = tableEnv.sqlQuery(
      """
        |select *
        |from (
        |   select *,row_number() over(partition by windowStart,windowEnd order by cnt desc) as rn
        |   from (
        |       select itemId,window_start as windowStart, window_end as windowEnd,count(itemId) as cnt
        |       from TABLE(
        |           HOP(TABLE hotItem, DESCRIPTOR(ts),interval '5' minutes, interval '1' hour))
        |       where behavior = 'pv'
        |       group by itemId,window_start, window_end
        |       )
        |   )
        |where rn <= 5
        |""".stripMargin)

    tableEnv.toDataStream(resultTable, classOf[Row]).print()
    env.execute("hot items with sql")
  }
}
