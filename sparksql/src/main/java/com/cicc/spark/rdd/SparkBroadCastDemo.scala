package com.cicc.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object SparkBroadCastDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkBroadCastDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val ints = List(1, 2, 3, 4, 5)
    val par = sc.parallelize(ints, 1)

    // 累加器：类似于mr的counter， 是在task中进行累加，然后在driver端进行汇总
    val acc = sc.accumulator(0)

    // 广播变量：类似于mr的distribute， 它是通过网络发送到每个Executor里，然后每个task不会重复的进行发送
    // 广播变量必须由可序列化的对象生成，因为通过网络传输到Executor去使用的。
    val br = sc.broadcast(ints)

    // 外部变量
    var num = 2

    val value = par.map(p => {
      // 使用的是外部变量，注意这个外部变量每个task都有其副本
      // 累加结果的时候不能使用外部变量，只能使用累加器，因为在分布式环境下，外部变量是不同步的。
      num = num + 1
      println(num)
      // 由于这个application有两个job，所以这个累加器被累加了两次
      // 如果RDD有N个Action并且他们都同时执行了累计器的累加动作的时候，这个累加器累计N次
      acc.add(1)
      // 这里使用的广播变量，executor中的task共享一个副本
      println(s"mapRDD : ${br.value}")
      p
    })

    // 执行了action foreach
    value.filter(f => {
      println(s"filterRDD：${br.value}")
      true
    }).foreach(println)

    // 执行了action count
    println(value.count())

    // 打印外部变量，在分布式环境下对其操作，是不会影响driver端的
    println(num)

    // 打印累加器，在分布式环境下进行汇总
    println(acc)

    sc.stop()

  }
}
