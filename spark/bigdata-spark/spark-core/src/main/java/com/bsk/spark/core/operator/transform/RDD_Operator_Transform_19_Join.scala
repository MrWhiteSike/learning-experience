package com.bsk.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_19_Join {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))

    val rdd = sc.makeRDD(List(("a",1),("b",2),("c",3),("d",4)))
    val rdd2 = sc.makeRDD(List(("a",5),("a",6),("e",8),("c",7)))

    // join : 两个不同数据源的数据，相同的key的value会连接在一起，形成元组。
    //  如果两个数据源中key没有匹配上，那么数据不会出现在结果中。
    //  如果两个数据源中key有多个相同的，会依次匹配，可能会出现笛卡尔积，数据量会几何性增长，会导致性能降低
    rdd.join(rdd2).collect().foreach(println)

    sc.stop()
  }

}
