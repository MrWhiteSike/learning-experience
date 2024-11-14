package com.cicc.spark.sql

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object SparkSqlHiveText {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSqlHiveOrc").setMaster("local[*]")
    conf.set("spark.sql.shuffle.partitions", "1")
    val sc = new SparkContext(conf)

    val orcPath = ""

    val hivec = new HiveContext(sc)
    val df = hivec.read.orc(orcPath)


    df.createOrReplaceTempView("s_table")

    // 保存成text 格式, 但是此格式不支持多字段，要在sql中多字段拼接成一字段，就可以保存出去了
    val sql = hivec.sql(
      """
        |select concat(name,"\t",num) as concat_string from
        |(select name,count(1) as num from s_table group by name) a
        |where age < 30 limit 2
        |""".stripMargin)

    // 注意：text 保存的时候，text 数据源只能保存一个列，多个列会报异常，解决方法：将多个列进行拼接

    // spark-sql 中的cache, 底层调用的是persist，所以缓存级别也是 MEMORY_AND_DISK，
    val cache = sql.cache()
    // DAG图也会跳过重复的运算
    cache.show()
    cache.write.mode(SaveMode.Overwrite).format("text").save("")
    cache.write.text("")

  }
}
