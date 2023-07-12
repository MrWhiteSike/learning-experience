package com.bsk.function

import com.bsk.bean.UserBehavior
import org.apache.flink.api.common.functions.MapFunction

import scala.util.Random

case class MyMapper(size: Int) extends MapFunction[UserBehavior, (String, Long)]{
  override def map(t: UserBehavior): (String, Long) = {
    (Random.nextString(size), 1L)
  }
}
