package com.cicc.spark.sql

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object SparkSqlHiveHql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparksqlhivehql").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val parquetPath = ""

    val hivec = new HiveContext(sc)

    // 创建库和表，if not exists 如果不存在就创建，存在自动退出
    hivec.sql("create database if not exists test")
    hivec.sql("use test")

    val createTableSql =
      """
        |CREATE TABLE IF NOT EXISTS sql_table
        |(
        |    name                    STRING COMMENT '序列号',
        |    server_brand            STRING COMMENT '服务器品牌',
        |    server_model            STRING COMMENT '服务器型号'
        |)
        |    COMMENT ''
        |    PARTITIONED BY (P_DT STRING)
        |    STORED AS PARQUET
        |    TBLPROPERTIES ('parquet.compression' = 'SNAPPY')
        |""".stripMargin

    hivec.sql(createTableSql)

    // load 数据到所在表sql_table下的数据目录中
    hivec.sql(s"load data local inpath '${parquetPath}' into table sql_table")

    // 对实体表进行sql语句查询
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


    /**
     * 元数据和数据的位置：
     * 如果没有hive-site.xml 配置，那会用derby数据库存放元数据，derby数据库会在当前项目目录
     *
     *
     * 如果没有配置warehouse的地址，那warehouse的生成目录也会在当前项目目录下，然后你创建的数据库和表的目录都在这个生成的warehouse目录里
     *
     */

  }
}
