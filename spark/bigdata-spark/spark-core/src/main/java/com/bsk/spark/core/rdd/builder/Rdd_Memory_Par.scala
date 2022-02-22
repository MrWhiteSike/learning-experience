package com.bsk.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Rdd_Memory_Par {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Rdd_Memory_Par")
//    conf.set("spark.default.parallelism", "6")
    val sc = new SparkContext(conf)

    // RDD 并行度 & 分区
    // makeRDD方法可以传递第二个参数，这个参数表示分区的数量
    // 第二个参数是可以不传递，会使用默认值， defaultParallelism
    // Spark在默认情况下，从配置对象中获取配置参数:spark.default.parallelism
    // 如果获取不到，那么使用totalCores属性，这个属性取值为当前运行环境的最大可用核数。
//    val rdd = sc.makeRDD(List(1,2,3,4),2)


    // 分区
    // 【1，2】【3，4】
//    val rdd = sc.makeRDD(List(1,2,3,4),2)

    // 【1】【2】【3，4】
//    val rdd = sc.makeRDD(List(1,2,3,4),3)

    // 【1】【2，3】【4，5】
    val rdd = sc.makeRDD(List(1,2,3,4,5),3)
    // 分区源码解析：
    // 调用了 ParallelCollectionRDD.slice[T : ClassTag](seq: Seq[T], numSlices: Int): Seq[Seq[T]] 方法
    // 将数组进行 def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] 操作
    // 获取了每个分区在数组中的（start，end）元素位置，然后通过（start，end）切割出数组对应位置的元素

    // 将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")
    sc.stop()
  }

}
