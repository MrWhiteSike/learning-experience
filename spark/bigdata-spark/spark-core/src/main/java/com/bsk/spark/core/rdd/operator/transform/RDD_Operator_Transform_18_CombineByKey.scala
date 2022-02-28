package com.bsk.spark.core.rdd.operator.transform

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
      v => (v, 1), // 转换为 tuple是在运行当中动态得到的，所以下面的tuple需要添加数据类型
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

    /**
     reduceByKey :
      分区内和分区间计算规则相同
      combineByKeyWithClassTag[V](
      (v: V) =>
       v,   // 第一个值不会参与计算
       func, // 分区内计算规则
       func, // 分区间计算规则
       partitioner)

     aggregateByKey :
      分区内和分区间计算规则不同
      combineByKeyWithClassTag[U](
     (v: V) =>
      cleanedSeqOp(createZero(), v), // 初始值和第一个key的value值进行的分区内数据操作
      cleanedSeqOp, // 分区内计算规则
     combOp, // 分区间计算规则
     partitioner)



     foldByKey :
      分区内和分区间处理函数相同
     combineByKeyWithClassTag[V]((v: V) =>
     cleanedFunc(createZero(), v), // 初始值和第一个key的value值进行的分区内数据操作
     cleanedFunc, // 表示分区内数据的处理函数
     cleanedFunc, // 表示分区间数据的处理函数
     partitioner)


     combineByKey :
      分区内和分区间处理函数不同
     combineByKeyWithClassTag(
     createCombiner, // 相同的key的第一条数据进行的处理函数
     mergeValue, // 表示分区内数据的处理函数
     mergeCombiners, // 表示分区间数据的处理函数
     defaultPartitioner(self))


     */


    sc.stop()
  }

}
