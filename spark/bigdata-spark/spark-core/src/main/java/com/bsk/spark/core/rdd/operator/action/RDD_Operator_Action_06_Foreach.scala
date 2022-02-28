package com.bsk.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Action_06_Foreach {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4),2)

    // foreach 其实是Driver端内存集合的循环遍历方法：分区有序拉取，输出有序
    rdd.collect().foreach(println)
    println("#######################")
    // foreach 其实是Executor端内存数据打印：分区内打印有序，分区间打印无序
    rdd.foreach(println)


    // 算子 ： Operator （操作）
    //        RDD的方法和Scala集合对象的方法不一样：
    //        集合对象的方法都是在同一个节点的内存中完成的
    //        RDD方法可以将计算逻辑发送到Executor端（分布式节点）执行的
    // 		  为了区分不同的处理效果，所以将RDD方法称之为算子
    //		  RDD 方法外部的操作都是在Driver端执行的，而方法内部的逻辑代码是在Executor端执行的。


    sc.stop()
  }

}
