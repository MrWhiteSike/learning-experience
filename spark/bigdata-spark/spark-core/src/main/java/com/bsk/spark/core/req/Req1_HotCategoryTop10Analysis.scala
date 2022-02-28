package com.bsk.spark.core.req

import org.apache.spark.{SparkConf, SparkContext}

object Req1_HotCategoryTop10Analysis {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(conf)
    // 1、读取原始日志数据
    val actionsRDD = sc.textFile("datas/user_visit_action.txt")

    // 2、统计品类的点击数量：（品类ID，点击数量）
    // 先过滤数据
    val clickActionRDD = actionsRDD.filter(
      action => {
        val datas = action.split("_")
        datas(6) != "-1"
      }
    )
    val clickCnt = clickActionRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

    // 3、统计品类的下单数量：（品类ID，下单数量）
    // 先过滤数据
    val orderActionRDD = actionsRDD.filter(
      action => {
        val datas = action.split("_")
        datas(8) != "null"
      }
    )

    val orderCnt = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cids = datas(8).split(",")
        cids.map(
          cid => (cid, 1)
        )
      }
    ).reduceByKey(_ + _)

    // 4、统计品类的支付数量：（品类ID，支付数量）
    // 先过滤数据
    val payActionRDD = actionsRDD.filter(
      action => {
        val datas = action.split("_")
        datas(10) != "null"
      }
    )
    val payCnt = payActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cids = datas(10).split(",")
        cids.map(
          cid => (cid, 1)
        )
      }
    ).reduceByKey(_ + _)

    // 5、将品类进行排序，并且取前10名
    //  点击数量排序，下单数量排序，支付数量排序
    //  元组排序：先比较第一个，再比较第二个，再比较第三个，以此类推
    // （品类ID，（点击数量，下单数量，支付数量））
    // cogroup : connect + group 连接在一起
    val cogroupRDD = clickCnt.cogroup(orderCnt, payCnt)
    val analysisRDD = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {
        var clickCount = 0
        var orderCount = 0
        var payCount = 0
        val iter1 = clickIter.iterator
        if (iter1.hasNext) {
          clickCount = iter1.next()
        }
        val iter2 = orderIter.iterator
        if (iter2.hasNext) {
          orderCount = iter2.next()
        }
        val iter3 = payIter.iterator
        if (iter3.hasNext) {
          payCount = iter3.next()
        }
        (clickCount, orderCount, payCount)
      }
    }

    val resultRDD = analysisRDD.sortBy(_._2, ascending = false).take(10)

    // 6、将结果采集到控制台打印出来
    resultRDD.foreach(println)


    sc.stop()
  }

}
