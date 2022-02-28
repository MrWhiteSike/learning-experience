package com.bsk.spark.core.broadcast

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object BroadCast_01 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Test")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val rdd1 = sc.makeRDD(List(("a",4),("b",5),("c",6)))
    val map = mutable.Map(("a",4),("b",5),("c",6))
    // join 会导致数据量 几何增长，并且会影响shuffle性能，不推荐使用
//    val joinRDD = rdd.join(rdd1)
//    joinRDD.collect().foreach(println)

    // 闭包数据，都是以Task 为单位发送的，每个任务中包含闭包数据。
    // 这样可能会导致，一个Executor中含有大量重复数据，并且占用大量的内存

    // Executor其实就是一个JVM，所以在启动时，会自动分配内存
    // 完全可以将任务中的闭包数据放置在Executor的内存中，达到共享的目的。

    // Spark中的广播变量，就可以将闭包数据保存到Executor的内存中
    // Spark 的广播变量不能够更改：分布式共享只读变量

    rdd.map{
      case (w,c) => {
        val l = map.getOrElse(w, 0L)
        (w,(c,l))
      }
    }.collect().foreach(println)

    sc.stop()
  }

}
