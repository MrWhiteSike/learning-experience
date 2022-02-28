package com.bsk.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_06_GroupBy {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))
    val rdd = sc.makeRDD(
      List(1, 2, 3, 4),2
    )

    // groupBy 会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组，
    // 相同的key值的数据会被放置在一个组中
    def groupFunction(num:Int):Int = {
      num % 2
    }

    val groupRDD = rdd.groupBy(groupFunction)

    groupRDD.collect().foreach(println)


    sc.stop()
  }

}
