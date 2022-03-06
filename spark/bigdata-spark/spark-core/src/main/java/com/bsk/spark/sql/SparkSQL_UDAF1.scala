package com.bsk.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession, functions}

object SparkSQL_UDAF1 {

  def main(args: Array[String]): Unit = {
    // TODO 创建Spark SQL的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // 隐式转换

    val df = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    // 使用 functions.udaf(agg) 方式。将强类型转换为弱类型的
    spark.udf.register("ageAvg", functions.udaf(new MyAvgUDAF()))
    spark.sql("select ageAvg(age) from user").show()
    // TODO 关闭环境
    spark.close()

  }

  // 自定义聚合函数类：计算年龄的平均值
  // 1、继承Aggregator
  // In：输入的数据类型 Long
  // BUF：
  // OUT：输出的数据类型Long
  // 2、重写方法

  case class Buff(var total:Long, var count:Long)
  class MyAvgUDAF extends Aggregator[Long, Buff, Long]{
    // 缓冲区初始化
    override def zero: Buff = {
      Buff(0L,0L)
    }

    // 根据输入的数据更新缓冲区数据
    override def reduce(b: Buff, a: Long): Buff = {
      b.total = b.total + a
      b.count = b.count + 1
      b
    }

    // 合并缓冲区
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total = b1.total + b2.total
      b1.count = b1.count + b2.count
      b1
    }

    // 计算结果
    override def finish(buff: Buff): Long = {
      buff.total / buff.count
    }

    // 缓冲区编码操作: 自定义类使用Encoders.product
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    // 输出的编码操作：scala 存在的类，比如，Long ==> Encoders.scalaLong
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }


}
