package com.bsk.function

import com.bsk.bean.{UserBehavior, UvCount}
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class MyUvCountResult() extends AllWindowFunction[UserBehavior, UvCount , TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    // 用一个集合来保存所有的userid，实现自动去重
    var idSet = Set[Long]()

    // 遍历所有数据，添加到Set中
    for (i <- input){
      idSet += i.userId
    }

    out.collect(UvCount(window.getEnd, idSet.size))
    
  }
}
