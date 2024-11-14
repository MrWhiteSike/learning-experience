package com.cicc.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

// 数据的bean
class SqlData(private var line:String){
  def getLine: String = {
    line
  }

  def setLine(line:String): Unit ={
    this.line = line
  }
}
object SparkSqlSchemaObject {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparksqljson").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val jsonPath = "C:\\Users\\Baisike\\project\\sparksql\\src\\main\\resources\\test.txt"

    // 对象对应表结构，所以这里的RDD中的类型必须与定义表结构的对象类型一致
    val rdd = sc.textFile(jsonPath).map(f =>  new SqlData(f))

    val sqlc = new SQLContext(sc)
    // 定义的表结构与数据集结合生成DF
    val df = sqlc.createDataFrame(rdd, classOf[SqlData])

    df.printSchema()

    // 创建临时表
    df.createOrReplaceTempView("sql_table")

    // 对临时表进行sql语句查询
    val sqlRes = sqlc.sql("select line from sql_table")

    sqlRes.show()


    // 转成RDD，把Row类型的RDD转成String类型的RDD
    val value = sqlRes.rdd.map(f => s"json_str:${f.getString(0)}")
    val jsonString = value.collect()
    jsonString.foreach(println)

    // RDD -> DataFrame -> table ->  DataFrame -> RDD

  }

}
