package com.bsk.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Action_05_Save {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)),2)

    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    // saveAsSequenceFile 方法要求数据的格式必须为 K-V 键值对类型
    rdd.saveAsSequenceFile("output2")

    sc.stop()
  }

}
