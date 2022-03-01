package com.bsk.spark.core.framework.application

import com.bsk.spark.core.framework.common.TApplication
import com.bsk.spark.core.framework.controller.WordCountController

/**
 * 应用层
 */
// 应用程序的入口程序
// 1、写main方法
// 2、继承 App
object WordCountApplication extends App with TApplication{
  // 启动应用程序
  start(){
    val wordCountController = new WordCountController()
    wordCountController.dispatch()
  }
}
