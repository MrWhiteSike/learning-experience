package com.bsk.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_17_AggregateByKey {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))

    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("a",4)),2)
    // 【("a",1),("a",2)】【("a",3),("a",4)】
    // 【("a",2)】【("a",4)】
    // 【("a",6)】

    // aggregateByKey 存在函数科里化，有两个参数列表
    // 第一个参数列表:需要传递一个参数，表示为初始值
    //      主要用于当碰见第一个key的时候，和value进行分区内计算
    // 第二个参数列表：
    //      第一个参数表示分区内计算规则
    //      第二个参数表示分区间计算规则
    rdd.aggregateByKey(0)(
      (x,y) => math.max(x,y),
      (x,y) => x + y
    ).collect().foreach(println)

    sc.stop()
  }

}
