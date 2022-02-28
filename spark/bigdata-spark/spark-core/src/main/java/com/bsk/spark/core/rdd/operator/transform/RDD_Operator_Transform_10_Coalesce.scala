package com.bsk.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_10_Coalesce {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))
    val rdd = sc.makeRDD(
      List(1, 2, 3, 4, 5, 6),3
    )

    // coalesce方法默认情况下不会将分区数据打乱重新组合
    // 这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
    // 如果想要让数据均衡，可以进行shuffle处理，添加第二个参数为true就可以了
//    val coaRDD = rdd.coalesce(2)
    val coaRDD = rdd.coalesce(2, true)
    coaRDD.saveAsTextFile("output1")

    sc.stop()
  }

}
