package com.bsk.spark.core.framework.controller

import com.bsk.spark.core.framework.common.TController
import com.bsk.spark.core.framework.service.WordCountServeice
import org.apache.spark.SparkContext

/**
 * 控制层
 */
class WordCountController extends TController{

  private val wordCountServeice = new WordCountServeice()

  // 调度
  def dispatch()={
    val array = wordCountServeice.dataAnalysis()
    // 属于调度（包括 输出到 文件 或者 控制台等）
    array.foreach(println)
  }
}
