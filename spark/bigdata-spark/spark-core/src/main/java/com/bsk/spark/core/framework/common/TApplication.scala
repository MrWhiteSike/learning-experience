package com.bsk.spark.core.framework.common

import com.bsk.spark.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {

  // ThreadLocal 可以对线程的内存进行控制，存储数据，共享数据; 创建一个工具类，可以对 sc 进行 存取、删除操作

  // scala 中有控制抽象的逻辑; 通过参数控制 环境和应用name
  def start(master:String="local[*]", app:String="Application")(op : => Unit) ={
    // TODO 建立和Spark框架的连接
    val conf = new SparkConf().setMaster(master).setAppName(app)
    val sc = new SparkContext(conf)
    EnvUtil.put(sc)
    try{
      op
    }catch {
      case ex => println(ex.getMessage)
    }
    // TODO 关闭连接
    sc.stop()
    // sc 不用了，需要清除
    EnvUtil.clear()
  }

}
