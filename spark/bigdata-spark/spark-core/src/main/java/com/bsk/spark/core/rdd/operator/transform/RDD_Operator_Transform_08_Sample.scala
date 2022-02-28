package com.bsk.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_08_Sample {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))
    val rdd = sc.makeRDD(
      List(1, 2, 3, 4),2
    )
    // sample算子需要传递三个参数
    // 第一个参数：表示抽取数据后是否将数据放回 true：放回，false：不放回
    // 第二个参数：
    //        如果抽取不放回的场合：表示数据源中每条数据被抽取的概率， 基准值的概念
    //        如果抽取放回的场合：表示数据源中的每条数据被抽取的可能次数
    // 第三个参数：表示抽取数据时随机算法的种子
    //        如果不传递第三个参数，那么使用的是当前系统时间
//    println(rdd.sample(
//      false,
//      0.4
////      1
//    ).collect().mkString((",")))

    println(rdd.sample(
      true,
      2
      //      1
    ).collect().mkString((",")))

    sc.stop()
  }

}
