package com.bsk.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_17_AggregateByKey {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))

    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("a",4)),2)
    // 【("a",1),("a",2)】【("a",3),("a",4)】
    // 【("a",2)】【("a",4)】
    // 【("a",6)】

    // aggregateByKey 存在函数柯里化，有两个参数列表
    // 第一个参数列表:需要传递一个参数，表示为初始值
    //      主要用于当碰见第一个key的时候，和value进行分区内计算
    // 第二个参数列表：
    //      第一个参数表示分区内计算规则
    //      第二个参数表示分区间计算规则
    // 如果初始值 设置的不同，可能会得到不同的结果

//    rdd.aggregateByKey(0)(
//      (x,y) => math.max(x,y),
//      (x,y) => x + y
//    ).collect().foreach(println)

    // 分区内和分区间的计算规则可以相同
//    rdd.aggregateByKey(0)(
//      (x,y) => x + y,
//      (x,y) => x + y
//    ).collect().foreach(println)

    // 简化写法
//    rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)

    // 如果聚合计算时，分区内和分区间计算规则相同，Spark提供了简化的方法
    rdd.foldByKey(0)(_+_).collect().foreach(println)

    sc.stop()
  }

}
