package com.cicc.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlJson {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparksqljson").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val jsonPath = "C:\\Users\\Baisike\\project\\sparksql\\src\\main\\resources\\test.txt"

    val sqlc = new SQLContext(sc)
    val df = sqlc.read.json(jsonPath)

    df.printSchema()
    df.show()

//    df.select(df.col("name")).show()
//
//    df.select(df.col("age").plus(1).alias("age_add")).show()
//
//    df.select(df.col("age").lt(23)).show()

    val count = df.groupBy("name").count()
    count.show()

    val rdd = count.rdd
    val coalRDD = rdd.coalesce(1)
    println(coalRDD.getNumPartitions)

    val outPath = "C:\\Users\\Baisike\\project\\sparksql\\src\\main\\resources\\output\\sqljson"

    coalRDD.saveAsTextFile(outPath)



  }

}
