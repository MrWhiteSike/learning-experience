package com.bsk.function

import com.bsk.bean.PvCount
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

case class MyTotalPvCount() extends KeyedProcessFunction[Long, PvCount, PvCount]{

  lazy val totalCountState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-count", classOf[Long]))


  override def processElement(i: PvCount, context: KeyedProcessFunction[Long, PvCount, PvCount]#Context, collector: Collector[PvCount]): Unit = {
    val currentTotalCount = totalCountState.value()
    totalCountState.update(currentTotalCount + i.count)
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
    out.collect(PvCount(ctx.getCurrentKey, totalCountState.value()))

    totalCountState.clear()
  }
}
