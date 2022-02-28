package com.bsk.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object RDD_Operator_Transform_15_ReduceByKey {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))

    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",2)))

    // reduceByKey 相同的Key的数据进行value数据的聚合操作
    // scala 语言中一般的聚合都是两两聚合，spark基于scala开发的，所以它的聚合也是两两聚合的

    // reduceByKey 中如果key的数据只有一个，是不会参与运算的。
    val reduceRDD = rdd.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    sc.stop()
  }

}
