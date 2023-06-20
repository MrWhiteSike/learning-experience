package com.bsk.function

import java.sql.Timestamp

import com.bsk.bean.PageViewCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 实现count结果排序，取TopN
 * @param topSize
 */
case class MyTopNHotPage(topSize: Int) extends KeyedProcessFunction[Long, PageViewCount, String]{

  // 方式 1 ：使用 ListState , 保存当前窗口的所有page的count值，存储了PageViewCount的所有信息的数据，相同的url可能会有多条数据
//  lazy val pageViewListState: ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pageview-count", classOf[PageViewCount]))

  // 改进：ListState需要的存储所有的数据，占用了大量的内存资源
  // 方式 2 ：使用 MapState，只存储 url 和 count的数据
  lazy val pageViewMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageView-count", classOf[String], classOf[Long]))

  override def processElement(value: PageViewCount, context: KeyedProcessFunction[Long, PageViewCount, String]#Context, collector: Collector[String]): Unit = {

//    pageViewListState.add(value)
    pageViewMapState.put(value.url, value.count)

    // 正常：没有开启 允许延迟 功能时，
    context.timerService().registerEventTimeTimer(value.windowEnd +1)

    // 定义一分钟之后的定时器，用于清除状态
//    context.timerService().registerEventTimeTimer(value.windowEnd + 60 * 1000L)
  }

  // 定时器触发执行操作
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //    // 判断时间戳，如果是1分钟后的定时器，直接清空状态
    //    if (timestamp == ctx.getCurrentKey + 60 * 1000L){
    //
    //    }

    // 定义ListBuffer，copy 所有状态数据用于排序
    val allPageViewCounts: ListBuffer[PageViewCount] = ListBuffer()



    /*
    // ListState
    val iter = pageViewListState.get().iterator()
    while (iter.hasNext){
      allPageViewCounts += iter.next()
    }
    // 清空状态数据
    pageViewListState.clear()
    */

    // MapState
    val iter = pageViewMapState.entries().iterator()
    while (iter.hasNext){
      val entry = iter.next()
      allPageViewCounts += PageViewCount(entry.getKey, timestamp - 1, entry.getValue)
    }

    // 对所有count值进行排序取TopN
    val sortedPageViewCounts = allPageViewCounts.sortWith(_.count > _.count).take(topSize)

    // 将排序数据封装成可视化的String，便于打印输出
    val result = new StringBuilder
    result.append("======================================\n")
    result.append("窗口结束时间：").append(new Timestamp(timestamp -1)).append("\n")
    for (i <- sortedPageViewCounts.indices){
      val currentView = sortedPageViewCounts(i)
      result.append("Number").append(i+1).append(":")
        .append(" 页面url=").append(currentView.url)
        .append(" 访问量=").append(currentView.count)
        .append("\n")
    }

    Thread.sleep(1000)
    out.collect(result.toString())

  }
}
