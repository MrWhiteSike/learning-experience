package com.bsk.spark.core.rdd.partitioner

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object RDD_Partitioner_01 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Test")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(
      ("nba", "xxxxxxxxxx"),
      ("cba", "xxxxxxxxxx"),
      ("wnba", "xxxxxxxxxx"),
      ("nba", "xxxxxxxxxx"),
    ),3)

    val partRDD = rdd.partitionBy(new MyPartitioner)
    partRDD.saveAsTextFile("output")
    
    sc.stop()
  }

  /**
   * 自定义分区器
   * 1、继承Partitioner
   * 2、重写方法
   */
  class MyPartitioner extends Partitioner{
    // 分区数量
    override def numPartitions: Int = 3

    // 根据数据的key值，返回数据的分区索引（从0开始）
    override def getPartition(key: Any): Int = {
      // 模式匹配 代替 多个if else 判断
      key match {
        case "nba" => 0
        case "cba" => 1
        case _ => 2
      }
//      if(key == "nba"){
//        0
//      } else if (key == "cba"){
//        1
//      } else if (key == "wnba"){
//        2
//      } else {
//        2
//      }
    }
  }

}
