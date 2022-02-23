package com.bsk.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_09_Distinct {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))
    val rdd = sc.makeRDD(
      List(1, 2, 3, 4, 2, 3, 1),2
    )
    // 参数为none时 distinct底层逻辑:  map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
    // map：（1，null）（2，null）（3，null）（4，null）（1，null）（2，null）（3，null）
    // reduceByKey:（1，null）（1，null）=> (null, null) => (null,null),null => (null,null) => (1,(null,null))
    // map: （1，null）=> 1
    // 使用分布式处理方式实现去重
    val distinctRDD = rdd.distinct()
    distinctRDD.collect().foreach(println)

    // 内存集合distinct的去重方式使用 HashSet 去重
    List(1,1,2,2).distinct

    sc.stop()
  }

}
