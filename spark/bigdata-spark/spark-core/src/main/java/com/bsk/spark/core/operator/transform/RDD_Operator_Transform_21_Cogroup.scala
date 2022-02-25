package com.bsk.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_21_Cogroup {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))

    val rdd = sc.makeRDD(List(("a",1),("b",2),("c",3),("d",4)))
    val rdd2 = sc.makeRDD(List(("a",5),("a",6),("e",8),("c",7)))

    // cogroup ： connect + group (分组，连接)
    // 可以有多个参数
    rdd.cogroup(rdd2).collect().foreach(println)

    sc.stop()
  }

}
