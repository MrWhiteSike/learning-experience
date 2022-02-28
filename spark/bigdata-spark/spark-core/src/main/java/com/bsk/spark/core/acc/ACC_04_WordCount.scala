package com.bsk.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object ACC_04_WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Test")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List("hello","spark","hello","scala"))

    // 创建累加器对象
    val accumulator = new MyAccumulator()
    // 向Spark 进行注册
    sc.register(accumulator, "wordCountAcc")

    rdd.foreach(
      word => {
        // 使用累加器
        accumulator.add(word)
      }
    )

    // 获取累加器累加的结果
    println(accumulator.value)

    sc.stop()
  }

  // 自定义数据累加器 : WordCount
  // 1、继承 AccumulatorV2,定义泛型
   // IN：累加器输入的数据类型 String
   // OUT：累加器输出的数据类型 mutable.Map[String, Long]
  // 2、重写方法
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]]{
    private var wcMap = mutable.Map[String, Long]()
    // 判断是否初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator
    }

    // 重置
    override def reset(): Unit = {
      wcMap.clear()
    }

    // 获取累加器需要计算的值
    override def add(word: String): Unit = {
      val newCnt = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word,newCnt)
    }

    // Driver合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.wcMap
      val map2 = other.value
      map2.foreach{
        case (word, count) => {
         val newCount =  map1.getOrElse(word, 0L) + count
          map1.update(word, newCount)
        }
      }
    }

    // 累加器结果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }

}
