package com.bsk.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_18_CombineByKey {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))

    val rdd = sc.makeRDD(List(("a",1),("a",2),("b",3),
      ("b",4),("b",5),("a",6)),2)

    // 获取相同key的数据的平均值 => (a,3) (b,4)

    // combineByKey 需要三个参数
    // 第一个参数表示：将相同key的第一个数据进行数据结构的转换，实现操作
    // 第二个参数表示：分区内的计算规则
    // 第三个参数表示：分区间的计算规则

    val newRDD = rdd.combineByKey(
      v => (v, 1), // 转换为的 tuple是在运行当中动态得到的，所以下面的tuple需要添加 数据类型
      (t:(Int, Int), v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1:(Int, Int), t2:(Int, Int)) => {
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
