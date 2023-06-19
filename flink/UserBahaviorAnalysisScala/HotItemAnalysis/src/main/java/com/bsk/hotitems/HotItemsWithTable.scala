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
      .window(Slide over 1.hours every 5.minutes on $"ts" as $"w")
      .groupBy($"itemId", $"w")
      .select(
        $"itemId",
        $"w".end() as "rt",
        $"itemId".count as "cnt"
      )

    table1.printSchema()

    //TODO Table 是否可以直接进行表结构的变更？ 肯定是可以的
    // 方式1；
    val table2 = table1.addColumns($"rt".cast(DataTypes.TIMESTAMP_LTZ(3)).as("windowEnd")).dropColumns($"rt")

    // 方式2：
//    val table1Stream = tEnv.toDataStream(table1)
//    val table2 = tEnv.fromDataStream(table1Stream,
//      Schema
//        .newBuilder()
//        .columnByExpression("windowEnd", "cast(rt as TIMESTAMP_LTZ(3))")
//        .build()
//    )
    table2.printSchema()


    /**
     * 问题：直接使用rt的时候出现如下异常
     * Exception in thread "main" java.lang.AssertionError: Conversion to relational algebra failed to preserve datatypes:
     * validated type:
     * RecordType(BIGINT NOT NULL itemId, TIMESTAMP(3) rt, BIGINT NOT NULL cnt) NOT NULL
     * converted type:
     * RecordType(BIGINT NOT NULL itemId, TIMESTAMP_LTZ(3) rt, BIGINT NOT NULL cnt) NOT NULL
     *
     * 解决：修改table1的表结构，增加数据类型为TIMESTAMP_LTZ(3)的列作为分区列
     *
     * table1.addColumns($"rt".cast(DataTypes.TIMESTAMP_LTZ(3)).as("windowEnd"))
     *
     *
     *
     */


    // 将聚合结果表注册到环境，写SQL实现top N
    tEnv.createTemporaryView("table1", table2)
    val result = tEnv.sqlQuery(
      """
        |select itemId,windowEnd,cnt,rn
        |from (
        |   select itemId,windowEnd,cnt,row_number() over(partition by windowEnd order by cnt desc) as rn
        |   from table1
        |)
        |where rn <= 5
        |""".stripMargin)

//    tEnv.toDataStream(table1, classOf[Row]).print("table1")


    /**
     * ROW_NUMBER 排序 是更新流，不能使用toDataStream 进行流表转换，需要使用toChangelogStream
     * Exception in thread "main" org.apache.flink.table.api.TableException:
     * Table sink '*anonymous_datastream_sink$4*' doesn't support consuming update and delete changes
     * which is produced by node Rank(strategy=[UndefinedStrategy], rankType=[ROW_NUMBER],
     * rankRange=[rankStart=1, rankEnd=5], partitionBy=[$2], orderBy=[cnt DESC], select=[itemId, cnt, $2, rn])
     */
//    tEnv.toDataStream(result, classOf[Row]).print("result")
    tEnv.toChangelogStream(result).print("result")


    env.execute()

  }
}
