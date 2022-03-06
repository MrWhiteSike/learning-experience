package com.bsk.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL_UDF {

  def main(args: Array[String]): Unit = {
    // TODO 创建Spark SQL的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // 隐式转换
    import spark.implicits._

    val df = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")
    // 给某字段数据加前缀
//    spark.sql("select age, 'username' + username from user").show()

    // UDF : 用户自定义函数，实现某些特殊功能的实现
    // 可以使用UDF来实现 加前缀 的功能
    spark.udf.register("prefixName", (name:String) => {
      "Name:"+name
    })
    spark.sql("select age,prefixName(username) from user").show()





    // TODO 关闭环境
    spark.close()

  }
}
