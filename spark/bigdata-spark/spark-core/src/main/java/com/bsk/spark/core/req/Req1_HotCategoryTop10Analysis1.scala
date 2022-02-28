package com.bsk.spark.core.req

import org.apache.spark.{SparkConf, SparkContext}

object Req1_HotCategoryTop10Analysis1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(conf)
    // 1、读取原始日志数据
    val actionsRDD = sc.textFile("datas/user_visit_action.txt")
    actionsRDD.cache()

    // Q：actionsRDD 重复使用
    // Q：cogroup性能可能较低
    // cogroup -> cogroupRDD -> 分区器不同，采用ShuffleDependency；有可能存在shuffle

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

    // 思路：
    // （品类ID，点击数量） => （品类ID，（点击数量，0，0））
    // （品类ID，下单数量） => （品类ID，（0，下单数量，0））
    // （品类ID，支付数量） => （品类ID，（0，0，支付数量））
    //                   两两聚合之后 => （品类ID，（点击数量，下单数量，支付数量））
    // 最终的形式：（品类ID，（点击数量，下单数量，支付数量））


    // 5、将品类进行排序，并且取前10名
    //  点击数量排序，下单数量排序，支付数量排序
    //  元组排序：先比较第一个，再比较第二个，再比较第三个，以此类推
    // （品类ID，（点击数量，下单数量，支付数量））

    // Q2解决：替换 cogroup 算子
    val map1 = clickCnt.map {
      case (cid, cnt) => {
        (cid, (cnt, 0, 0))
      }
    }
    val map2 = orderCnt.map {
      case (cid, cnt) => {
        (cid, (0, cnt, 0))
      }
    }

    val map3 = payCnt.map {
      case (cid, cnt) => {
        (cid, (0, 0, cnt))
      }
    }

    // 将三个数据源合并在一起，统一进行聚合计算
    val analysisRDD = map1.union(map2).union(map3).reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    val resultRDD = analysisRDD.sortBy(_._2, ascending = false).take(10)

    // 6、将结果采集到控制台打印出来
    resultRDD.foreach(println)


    sc.stop()
  }

}
