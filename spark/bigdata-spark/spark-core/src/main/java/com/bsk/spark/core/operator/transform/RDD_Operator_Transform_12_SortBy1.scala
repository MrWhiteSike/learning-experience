package com.bsk.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_11_SortBy1 {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))
    val rdd = sc.makeRDD(
      List(("1",1),("11",3),("2",2)),2
    )
    // sortBy 算子可以根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以改变排序方式
    // sortBy 默认情况下，不会改变分区，但是中间存在shuffle操作
    val sortRDD = rdd.sortBy(t => t._1.toInt)
    sortRDD.collect().foreach(println)

    sc.stop()
  }

}
