package com.bsk.function

import com.bsk.bean.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction

/**
 * 需求：热门商品统计TopN
 * 作用：自定义预聚合函数，每来一个数据就count + 1
 *      将count结构输出
 */
case class MyCountAggFunction() extends AggregateFunction[UserBehavior, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(a: Long, b: Long): Long = a + b
}
