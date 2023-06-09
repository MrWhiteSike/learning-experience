package com.bsk.function

import java.sql.Timestamp

import com.bsk.bean.ItemViewCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 需求：热门商品统计
 * 作用：自定义一个KeyedProcessFunction，对每个窗口的count统计值排序，并格式化字符串输出
 * @param topSize N
 */
case class MyTopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String]{

  // 定义列表状态，用来保存当前窗口的所有商品的count
  private var itemViewCountListState: ListState[ItemViewCount] = _

  // 初始化列表状态
  override def open(parameters: Configuration): Unit = {
    itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCount-liststate", classOf[ItemViewCount]))
  }

  // 保存每一条ItemViewCount数据
  override def processElement(value: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    //
    itemViewCountListState.add(value)
    // 需要注册一个windowEnd + 1的定时器
    context.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 将 ListState 中的数据，全部放到一个ListBuffer中，方便排序
    val allItemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()

    // 遍历添加
    import scala.collection.JavaConverters._
    for (itemViewCount <- itemViewCountListState.get().asScala){
      allItemViewCounts += itemViewCount
    }

    // 数据取出来存在ListBuffer后，可以清空状态
    itemViewCountListState.clear()

    // 排序 反转逆序 取出前topSize
    val sortedItemViewCounts = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 将排序数据包装成可视化的String，便于打印
    val result = new StringBuilder

    result.append("================================================\n")
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedItemViewCounts.indices){
      val currentViewCount = sortedItemViewCounts(i)
      result.append("NO").append(i+1).append(":")
        .append(" 商品ID = ").append(currentViewCount.itemId)
        .append(" 点击量 = ").append(currentViewCount.count)
        .append("\n")
    }

    // 控制输出频率
    Thread.sleep(1000)

    out.collect(result.toString())
  }
}
