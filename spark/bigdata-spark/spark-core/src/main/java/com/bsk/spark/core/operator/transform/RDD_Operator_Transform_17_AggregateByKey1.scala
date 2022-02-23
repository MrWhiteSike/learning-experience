package com.bsk.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_17_AggregateByKey1 {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))

    val rdd = sc.makeRDD(List(("a",1),("a",2),("b",3),
      ("b",4),("b",5),("a",6)),2)

    // aggregateByKey 最终的返回数据结果应该和初始值的类型保持一致

    // 获取相同key的数据的平均值 => (a,3) (b,4)

    // 初始值传一个tuple （0，0），第一个数代表value总和，第二个数代表key出现的次数；初始值都设置为0
    val newRDD = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    val resultRDD = newRDD.mapValues {
      case (sum, cnt) => sum / cnt
    }

    resultRDD.collect().foreach(println)

    sc.stop()
  }

}
