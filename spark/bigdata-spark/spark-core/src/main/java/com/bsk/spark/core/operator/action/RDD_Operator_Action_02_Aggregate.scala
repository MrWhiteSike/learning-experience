package com.bsk.spark.core.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Action_02_Aggregate {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4),2)

    // aggregate : 分区的数据通过 初始值 和分区内的数据进行聚合，然后再和 初始值 进行分区间的数据聚合

    // aggregateByKey ：初始值只会参与分区内计算
    // aggregate ：初始值会参与分区内计算，并且还参与分区间计算
//    val i = rdd.aggregate(0)(
//      (x, y) => Math.max(x, y),
//      (t1, t2) => t1 + t2
//    )

    // 10(分区间的初始值) + 13（第一个分区内的计算结果） +  17（第二个分区内的计算结果） = 40
//    val i = rdd.aggregate(10)(_+_,_+_)

    // fold ：aggregate 简化版，分区内和分区间计算规则相同时
    val i = rdd.fold(10)(_+_)



    println(i)

    sc.stop()
  }

}
