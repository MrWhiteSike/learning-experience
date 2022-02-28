package com.bsk.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_20_LeftOuterJoin {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))

    val rdd = sc.makeRDD(List(("a",1),("b",2),("c",3),("d",4)))
    val rdd2 = sc.makeRDD(List(("a",5),("a",6),("e",8),("c",7)))

    // leftOuterJoin 类似于SQL中左外连接，左表所有的元素都会出现，右表的匹配元素出现
    // rightOuterJoin类似于SQL中右外连接；右表所有的元素都会出现，左表的匹配元素出现
    rdd.leftOuterJoin(rdd2).collect().foreach(println)
    rdd.rightOuterJoin(rdd2).collect().foreach(println)

    sc.stop()
  }

}
