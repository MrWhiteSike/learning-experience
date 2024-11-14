package com.cicc.spark.sql

import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{RowFactory, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object SparkSqlSchema {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparksqljson").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val jsonPath = "C:\\Users\\Baisike\\project\\sparksql\\src\\main\\resources\\test.txt"

    // 生成RDD[ROW]类型数据
    val map = sc.textFile(jsonPath).map(f => RowFactory.create(f))

    // 自定义字段类型
    val fields = new ArrayBuffer[StructField]()
    fields += DataTypes.createStructField("line", DataTypes.StringType, true)
    // 使用了自定义的字段类型创建表结构
    val tableSchema = DataTypes.createStructType(fields.toArray)

    val sqlc = new SQLContext(sc)

    // 使用自定义的表结构与RDD[Row]进行结合生成DF
    val df = sqlc.createDataFrame(map, tableSchema)
    df.printSchema()

    // 对DF进行sql查询操作
    val filterDF = df.filter(df.col("line").like("%jack%"))
    filterDF.show()
    val l = filterDF.count()
    println(s"filter count = ${l}")


  }
}
