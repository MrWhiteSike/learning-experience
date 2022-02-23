package com.bsk.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object RDD_Operator_Transform_14_PartitionsBy {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val newRDD:RDD[(Int, Int)] = rdd.map((_, 1))

    // RDD =>> PairRDDFunctions
    // 隐式转换（二次编译）: RDD 调用了伴生对象中的隐式转换方法：implicit def rddToPairRDDFunctions 进行转换的。

    // partitionBy 根据指定的分区规则对数据进行重分区
    newRDD.partitionBy(new HashPartitioner(2))
      .saveAsTextFile("output")


    sc.stop()
  }

}
