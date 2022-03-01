package com.bsk.spark.core.req

import org.apache.spark.{SparkConf, SparkContext}

object Req3_PageBowAnalysis {

  /**
   * 需求3： 页面单跳转化率统计
   * @param args
   */

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(conf)
    // 1、读取原始日志数据
    val actionsRDD = sc.textFile("datas/user_visit_action.txt")
    actionsRDD.cache()

    // 转换为 UserVisitAction 结构数据
    val dataRDD = actionsRDD.map(
      action => {
        val datas = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )

    // TODO 对指定的页面连续跳转进行统计
    // 分母需要过滤
    // 1-2，2-3，3-4，4-5，5-6，6-7
    val ids = List[Long](1,2,3,4,5,6,7)

    val okFlowIds = ids.zip(ids.tail)

    // TODO 计算分母
    // 分析：分母之和 page_id 有关，就是对page_id做wordcount，然后采集结果
    val pageIdToCountArray = dataRDD.filter(
      action => {
        // scala 集合当中的init ：不包含最后一个元素
        ids.init.contains(action.page_id)
      }
    ).map(
      action => {
        (action.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap


    // TODO 计算分子
    // 分析：先根据 sessionID 分组，然后对 action_time 排序，转换数据结构(（page1,page2）,1)，聚合结果得到分子
    // 我的实现；
    /*
    val groupRDD = dataRDD.map(
      action => (action.session_id, (action.action_time, action.page_id))
    ).groupByKey()

    val sortRDD = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._1)
      }
    )

    val pageToPageCountRDD = sortRDD.flatMap(
      action => {
        val list = action._2
        val list1 = list.take(list.size - 1)
        val list2 = list.takeRight(list.size - 1)
        list1.zip(list2).map(
          t => {
            (t._1._2,t._2._2)
          }
        )
      }
    ).map((_, 1)).reduceByKey(_ + _)

    // 采集打印
    pageToPageCountRDD.collect{
     case ((page1, page2), cnt) => {
       val l = pageIdToCountArray.getOrElse(page1, 0L)
       ((page1, page2),cnt.toDouble/l)
      }
    }.foreach(println)
    */

    // 教程实现

    // 根据 session 分组
    val sessionRDD = dataRDD.groupBy(_.session_id)
    // 根据 action_time 排序
    val mvRDD = sessionRDD.mapValues(
      iter => {
        val sortList = iter.toList.sortBy(_.action_time)

        // 【1，2，3，4】 => 【1-2，2-3，3-4】的实现方式：
        // Sliding ：滑窗
        // 【1，2，3，4】
        // 【2，3，4】
        // zip ：拉链
        val flowIds = sortList.map(_.page_id)
        val pageFlowIds = flowIds.zip(flowIds.tail)
        // 将不合法的页面跳转进行过滤
        pageFlowIds.filter(
          t => {
            okFlowIds.contains(t)
          }
        ).map(
          t => (t,1)
        )
      }
    )

    // 数据结构转换: （sid,List[((pid1,pid2),1)]）=> List[((pid1,pid2),1)] => ((pid1,pid2),1)
    val flatRDD = mvRDD.map(_._2).flatMap(list => list)

    // 聚合
    val flowToCountRDD = flatRDD.reduceByKey(_ + _)

    // TODO 计算单跳转化率
    flowToCountRDD.foreach{
      case ((page1,page2),sum) => {
        val lon = pageIdToCountArray.getOrElse(page1, 0L)
        println(s"页面${page1}跳转到页面${page2}单跳转换率为：" + (sum.toDouble/lon))
      }
    }

    sc.stop()
  }

  // 用户访问动作表
  case class UserVisitAction(
    date:String, // 用户点击行为的日期
    user_id:Long, // 用户ID
    session_id:String, // Session ID
    page_id:Long, // 某个页面的ID
    action_time:String, // 动作的时间点
    search_keyword:String, // 用户搜索的关键词
    click_category_id:Long, // 某一个商品品类的ID
    click_product_id:Long, // 某个商品的ID
    order_category_ids:String, // 一次订单中所有品类的ID集合
    order_product_ids:String, // 一次订单中所有商品的ID集合
    pay_category_ids:String, // 一次支付中所有品类的ID集合
    pay_product_ids:String, // 一次支付中所有商品的ID集合
    city_id:Long // 城市ID
  )



}
