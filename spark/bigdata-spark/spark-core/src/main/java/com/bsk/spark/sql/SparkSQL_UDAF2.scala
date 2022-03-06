package com.bsk.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

object SparkSQL_UDAF2 {

  def main(args: Array[String]): Unit = {
    // TODO 创建Spark SQL的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val df = spark.read.json("datas/user.json")
    // 早期版本中，spark不能在sql中使用强类型UDAF操作
    // SQL & DSL

    // 早期的UDAF强类型聚合函数使用DSL语法操作
    val ds = df.as[User]

    // 将UDAF函数转换为查询的列对象
    val udafColumn = new MyAvgUDAF().toColumn

    ds.select(udafColumn).show()

    // TODO 关闭环境
    spark.close()

  }

  // 自定义聚合函数类：计算年龄的平均值
  // 1、继承Aggregator
  // In：输入的数据类型 Long
  // BUF：
  // OUT：输出的数据类型Long
  // 2、重写方法

  case class User(username:String, age:Long)
  case class Buff(var total:Long, var count:Long)
  class MyAvgUDAF extends Aggregator[User, Buff, Long]{
    // 缓冲区初始化
    override def zero: Buff = {
      Buff(0L,0L)
    }

    // 根据输入的数据更新缓冲区数据
    override def reduce(b: Buff, a: User): Buff = {
      b.total = b.total + a.age
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
