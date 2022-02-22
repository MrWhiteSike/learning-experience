package com.bsk.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))
    val rdd = sc.makeRDD(
      List(1, 2, 3, 4)
    )
    // 转换函数
    def mapFunction(num:Int): Int = {
      num * 2
    }

//    val mapRDD = rdd.map(mapFunction)
    // 自检原则：
    // 匿名函数
//    val mapRDD = rdd.map((num:Int) => {num*2})
    // 简化：当代码只有一行的时候，{}可以省略
//    val mapRDD = rdd.map((num:Int) => num*2)
    // 简化：如果数据类型可以自动推断出来的话，数据类型可以省略
//    val mapRDD = rdd.map((num) => num*2)
    // 简化：如果参数列表只有一个的时候，()可以省略
//    val mapRDD = rdd.map(num => num*2)
    // 简化：参数在逻辑当中只出现一次，并且按顺序出现的时候，参数可以用_代替
    val mapRDD = rdd.map(_*2)


    mapRDD.collect().foreach(println)

    sc.stop()
  }

}
