package com.bsk.spark.core.framework.util

import org.apache.spark.SparkContext

/**
 * 利用 ThreadLocal 实现环境变量存取的工具类
 */
object EnvUtil {
  private val scLocal = new ThreadLocal[SparkContext]
  def put(sc : SparkContext)={
    scLocal.set(sc)
  }

  def take()={
    scLocal.get()
  }

  def clear()={
    scLocal.remove()
  }


}
