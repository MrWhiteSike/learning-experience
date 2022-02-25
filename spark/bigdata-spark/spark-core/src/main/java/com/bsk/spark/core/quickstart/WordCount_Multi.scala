package com.bsk.spark.core.quickstart

import org.apache.spark.{SparkConf, SparkContext}

object WordCount_Multi {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("WordCount"))
    wordcount(sc)



    sc.stop()

  }

  // groupBy
  def wordcount(sc: SparkContext): Unit ={
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val mapRDD = rdd.flatMap(_.split(" "))
    val mRDD = mapRDD.map((_, 1))
    val group = mRDD.groupBy(t => t._1)
    val wordcount = group.mapValues(
      iter => iter.size
    )
  }

  // groupByKey : 分组的方式，存在shuffle，性能不高
  def wordcount2(sc: SparkContext): Unit ={
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val mapRDD = rdd.flatMap(_.split(" "))
    val mRDD = mapRDD.map((_, 1))
    val group = mRDD.groupByKey()
    val wordcount = group.mapValues(
      iter => iter.size
    )
  }

  // reduceByKey : 两两聚合性能比较高
  def wordcount3(sc: SparkContext): Unit ={
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val mapRDD = rdd.flatMap(_.split(" "))
    val mRDD = mapRDD.map((_, 1))
    val wordcount = mRDD.reduceByKey(_+_)
  }

  // aggregateByKey
  def wordcount4(sc: SparkContext): Unit ={
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val mapRDD = rdd.flatMap(_.split(" "))
    val mRDD = mapRDD.map((_, 1))
    val wordcount = mRDD.aggregateByKey(0)(_+_,_+_)
  }

  // foldByKey : aggregateByKey 的简化版，分区间和分区内计算规则相同
  def wordcount5(sc: SparkContext): Unit ={
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val mapRDD = rdd.flatMap(_.split(" "))
    val mRDD = mapRDD.map((_, 1))
    val wordcount = mRDD.foldByKey(0)(_+_)
  }

  // combineByKey : 三个参数，第一个参数：第一数据值的处理规则，第二参数：分区内，第三个参数：分区间
  def wordcount6(sc: SparkContext): Unit ={
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val mapRDD = rdd.flatMap(_.split(" "))
    val mRDD = mapRDD.map((_, 1))
    val wordcount = mRDD.combineByKey(
      v => v,
      (x:Int, y) => x + y,
      (x:Int, y:Int) => x + y
    )
  }

  // countByKey
  def wordcount7(sc: SparkContext): Unit ={
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val mapRDD = rdd.flatMap(_.split(" "))
    val mRDD = mapRDD.map((_, 1))
    val wordcount = mRDD.countByKey()
  }

  // countByValue
  def wordcount8(sc: SparkContext): Unit ={
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val mapRDD = rdd.flatMap(_.split(" "))
    val wordcount = mapRDD.countByValue()
  }


}
