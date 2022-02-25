package com.bsk.spark.core.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Action_01_Reduce {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4))

    // 行动算子，其实就是触发作业执行的方法
    // 底层代码调用的是环境对象中 runJob 方法，调用dagScheduler的runJob方法创建ActiveJob，并提交执行。

    // reduce
//    val i = rdd.reduce(_ + _)
//    println(i)


    // collect : 采集，该方法会将不同分区间的数据按照分区顺序采集到Driver端，形成数组
//    val ints = rdd.collect()
//    ints.foreach(println)


    // count : 数据源中数据个数
//    val l = rdd.count()
//    println(l)

    // first : 获取数据源中数据的第一个
//    val i = rdd.first()
//    println(i)


    // take : 获取数据源中数据的前N个
    val ints = rdd.take(2)
    println(ints.mkString(","))

    // takeOrdered : 数据先排序，再取N个，默认升序排序，可以使用第二个参数列表（比如 ： Ordering.Int.reverse）实现倒序功能
    val rdd1 = sc.makeRDD(List(4,3,2,1))
    val ints1 = rdd1.takeOrdered(2)(Ordering.Int.reverse)
    println(ints1.mkString(","))

    sc.stop()
  }

}
