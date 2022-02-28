package com.bsk.spark.core.req

import org.apache.spark.{SparkConf, SparkContext}

object Req1_HotCategoryTop10Analysis2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(conf)
    // 1、读取原始日志数据
    val actionsRDD = sc.textFile("datas/user_visit_action.txt")
    // Q： 存在大量的shuffle操作（reduceByKey）
    // reduceByKey 聚合算子，spark会提供优化，和底层有缓存操作
    // 2、将数据转换结构
    // 思路：
    // 点击的场合 => （品类ID，（1，0，0））
    // 下单的场合 => （品类ID，（0，1，0））
    // 支付的场合 => （品类ID，（0，0，1））
    //              两两聚合之后 => （品类ID，（点击数量，下单数量，支付数量））
    // 最终的形式：（品类ID，（点击数量，下单数量，支付数量））

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
          // 返回空
          Nil
        }
      }
    )

    // 3、将相同的品类ID的数据进行分组聚合
    val analysisRDD = flatRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    // 4、将品类进行排序，并且取前10名
    //  点击数量排序，下单数量排序，支付数量排序
    //  元组排序：先比较第一个，再比较第二个，再比较第三个，以此类推
    // （品类ID，（点击数量，下单数量，支付数量））
    val resultRDD = analysisRDD.sortBy(_._2,false).take(10)

    // 5、将结果采集到控制台打印出来
    resultRDD.foreach(println)


    sc.stop()
  }

}
