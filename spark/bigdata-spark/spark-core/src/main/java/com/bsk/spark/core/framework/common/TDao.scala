package com.bsk.spark.core.framework.common

import com.bsk.spark.core.framework.util.EnvUtil

// 对 Dao 层的抽象
trait TDao {
  def loadData(path:String)={
    val sc = EnvUtil.take()
    // 1、读取文件，按行读取
    sc.textFile(path)
  }
}
