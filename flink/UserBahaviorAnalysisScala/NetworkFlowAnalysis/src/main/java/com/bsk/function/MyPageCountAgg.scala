package com.bsk.function

import com.bsk.bean.ApacheLogEvent
import org.apache.flink.api.common.functions.AggregateFunction

/**
 * 热门页面统计
 */
case class MyPageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long]{
  // 初始值
  override def createAccumulator(): Long = 0L

  // 来一条 +1
  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  // 返回聚合结果
  override def getResult(acc: Long): Long = acc

  // 聚合操作
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
