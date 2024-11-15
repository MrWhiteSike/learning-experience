package com.bsk.function

import com.bsk.bean.ItemViewCount
import org.apache.flink.api.java.tuple.{Tuple,Tuple1}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 需求：热门商品统计TopN
 * 作用：自定义一个全窗口函数，将窗口信息包装进去输出
 */
case class MyWindowResultFunction() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    // 获取itemId
    val itemId = key
    // 获取窗口结束时间
    val windowEnd = window.getEnd
    // 获取计数信息 迭代器中只有一个数，直接取
    val count = input.iterator.next()
    // 封装并输出
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}
