package com.bsk.spark.sql

import org.apache.parquet.filter2.predicate.Operators.UserDefined
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

object SparkSQL_UDAF {

  def main(args: Array[String]): Unit = {
    // TODO 创建Spark SQL的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // 隐式转换

    val df = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    // 可以使用UDAF来实现:求平均值
    spark.udf.register("ageAvg", new MyAvgUDAF())
    spark.sql("select ageAvg(age) from user").show()
    // TODO 关闭环境
    spark.close()

  }

  // 自定义聚合函数类：计算年龄的平均值
  // 1、继承UserDefinedAggregateFunction
  // 2、重写方法
  class MyAvgUDAF extends UserDefinedAggregateFunction{
    // 输入数据的结构：In
    override def inputSchema: StructType = {
      StructType(
        Array(
          StructField("age", LongType)
        )
      )
    }

    // 缓冲区数据的结构：Buffer
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("age", LongType),
          StructField("count", LongType)
        )
      )
    }

    // 函数计算结果的数据类型：Out
    override def dataType: DataType = LongType

    // 函数的稳定性
    override def deterministic: Boolean = true

    // 缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      // 方式1：
//      buffer(0) = 0L
//      buffer(1) = 0L

      // 方式2：
      buffer.update(0, 0L)
      buffer.update(1, 0L)
    }

    // 根据输入的值更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0, buffer.getLong(0) + input.getLong(0))
      buffer.update(1, buffer.getLong(1)+1)
    }

    // 缓冲区数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
    }

    // 计算平均值
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }


}
