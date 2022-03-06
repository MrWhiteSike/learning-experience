package com.bsk.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL_Hive {

  def main(args: Array[String]): Unit = {
    // TODO 创建Spark SQL的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    // 使用SparkSql 连接外置的Hive
    /*
    * 1、拷贝hive-site.xml文件到classpath下
    * 2、启用Hive的支持
    * 3、增加对应的依赖关系（包含MySQL驱动）
    *
    * */
    spark.sql("show tables").show()

    // TODO 关闭环境
    spark.close()

  }
}
