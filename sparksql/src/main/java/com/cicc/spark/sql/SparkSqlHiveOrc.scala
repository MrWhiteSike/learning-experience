package com.cicc.spark.sql

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlHiveOrc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSqlHiveOrc").setMaster("local[*]")
    conf.set("spark.sql.shuffle.partitions", "1")
    val sc = new SparkContext(conf)

    val orcPath = ""

    val hivec = new HiveContext(sc)
    val df = hivec.read.orc(orcPath)

    df.select(df.col("name")).show(5)

    df.select(df.col("name").as("name2")).show(5)

    val count = df.groupBy("name").count()
    count.printSchema()

    val select = count.select(count.col("name"), count.col("count").as("num"))
    select.printSchema()

    val selectCount = select.filter(select.col("num").gt(30)).limit(10)
    selectCount.show()

    // 注意： DF的默认缓存级别是 Memory_And_Disk, 而RDD默认缓存级别的是Memory_Only ， 使用的时候要注意区别
    val cache = selectCount.persist()

    // 上面使用了cache，下面的多个job不会重复运算
    // 使用了ds定义好的格式保存方法
    cache.write.mode(SaveMode.Overwrite).format("orc").save("")
    cache.write.mode(SaveMode.Overwrite).format("json").save("")
    cache.write.mode(SaveMode.Overwrite).format("hive").saveAsTable("")


    cache.write.mode(SaveMode.Overwrite).save()
    cache.write.mode(SaveMode.Overwrite).parquet("")
    cache.write.mode(SaveMode.Overwrite).csv("")
    cache.write.mode(SaveMode.Overwrite).json("")
    cache.write.mode(SaveMode.Overwrite).orc("")
    cache.write.mode(SaveMode.Overwrite).text("")

  }
}
