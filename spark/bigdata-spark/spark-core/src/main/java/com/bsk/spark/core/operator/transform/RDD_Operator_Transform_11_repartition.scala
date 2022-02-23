package com.bsk.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_11_repartition {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))
    val rdd = sc.makeRDD(
      List(1, 2, 3, 4, 5, 6),2
    )
    // coalesce 算子可以扩大分区的，但是如果不进行shuffle操作，是没有意义的，不起作用的，必须设置第二个参数为true才起作用。
//    val coaRDD = rdd.coalesce(3)
//    val coaRDD = rdd.coalesce(3, true)

    // Spark提供了简化的操作
    // 缩减分区：coalesce，如果想要数据均衡，可以采用shuffle
    // 扩大分区：repartition 底层代码就是调用 coalesce，而且肯定shuffle
    val coaRDD = rdd.repartition(3)



    coaRDD.saveAsTextFile("output")

    sc.stop()
  }

}
