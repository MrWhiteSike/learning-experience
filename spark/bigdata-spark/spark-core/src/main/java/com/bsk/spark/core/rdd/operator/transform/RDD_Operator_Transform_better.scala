package com.bsk.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_better {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))
    val rdd = sc.makeRDD(
      List(1, 2, 3, 4),2
    )

    // mapPartitions ：可以以分区为单位进行数据转换操作
    //      但是，会将整个分区的数据加载到内存进行引用
    //      如果处理完的数据是不会被释放的，存在对象的引用
    //      在内存较小，数据量较大的场合下，容易出现内存溢出

//    val mpRDD = rdd.mapPartitions(
//      iter => {
//        println(">>>>>>>>>>")
//        // 内存中的操作，性能比较高
//        iter.map(_*2)
//      }
//    )


    // 特殊功能实现：取出每个分区中的最大值
    val mpRDD = rdd.mapPartitions(
      iter => {
        // iter.max 是一个值，不是一个迭代器；不过可以在外层包装一层，返回迭代器；
        List(iter.max).iterator
      }
    )





    mpRDD.collect().foreach(println)

    sc.stop()
  }

}
