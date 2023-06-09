package com.bsk.hotitems

import com.bsk.bean.UserBehavior
import com.bsk.function.{MyCountAggFunction, MyTopNHotItems, MyWindowResultFunction}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

// 导入隐式转换
import org.apache.flink.streaming.api.scala._

object HotItemsWithFromKafkaSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 配置kafka消费者
    val brokers = "172.17.128.1:9092"
    val source = KafkaSource.builder()
      .setBootstrapServers(brokers)
      .setTopics("test")
      .setGroupId("consumer-group")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    // 从Kafka读取数据
    val inputDStream = env.fromSource(source,
                              WatermarkStrategy.noWatermarks(),
                  "kafka source")

    // 转换为样例类
    val dataDStream = inputDStream.map(data => {
      val dataArr = data.split(",")
      UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)

    // 过滤 pv ，按商品ID分组，创建窗口大小1小时，滑动间隔5分钟的滑动窗口，计算每个窗口中商品的统计信息
    val aggDStream = dataDStream
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
      .aggregate(MyCountAggFunction(), MyWindowResultFunction())

    // 按照窗口结束时间分组，计算TopN
    val result = aggDStream
      .keyBy(_.windowEnd)
      .process(MyTopNHotItems(5))

    // 打印输出
    result.print()
    env.execute("kafka source hot items job")
  }

}
