package com.bsk.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req2_HotCategoryTop10SessionAnalysis {

  /**
   * 需求2： Top10热门品类中 每个品类 的Top10活跃Session统计
   * 说明：在需求1 的基础上，增加每个品类用户 session的点击统计
   * @param args
   */

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(conf)
    // 1、读取原始日志数据
    val actionsRDD = sc.textFile("datas/user_visit_action.txt")
    actionsRDD.cache()
    // 2、Top10热门品类
    val top10 = top10Cids(actionsRDD)

    // 3、（（品类ID，Session），sum）
    val filterRDD = actionsRDD.filter(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          top10.contains(datas(6))
        } else {
          false
        }
      }
    )
    val reduceRDD = filterRDD.map(
      action => {
        val datas = action.split("_")
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _)

    // 4、数据格式转换
    val mapRDD = reduceRDD.map {
      case ((cid, sid), sum) => {
        (cid, (sid, sum))
      }
    }
    // 5、按照品类ID分组，倒序排序，取前10名
    val groupRDD = mapRDD.groupByKey()
    val resultRDD = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    )

    // 6、采集打印到控制台
    resultRDD.collect().foreach(println)

    sc.stop()
  }

  def top10Cids(actionsRDD:RDD[String]): Array[String] ={
    val flatRDD = actionsRDD.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          // 点击的场合
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          // 下单的场合
          val cids = datas(8).split(",")
          cids.map(cid => (cid, (0, 1, 0)))
        } else if (datas(10) != "null") {
          // 支付的场合
          val cids = datas(10).split(",")
          cids.map(cid => (cid, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )
    val analysisRDD = flatRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    analysisRDD.sortBy(_._2,false).take(10).map(_._1)
  }

}
