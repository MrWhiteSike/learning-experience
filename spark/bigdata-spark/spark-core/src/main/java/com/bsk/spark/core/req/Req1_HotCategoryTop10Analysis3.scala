package com.bsk.spark.core.req

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Req1_HotCategoryTop10Analysis3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(conf)
    // 1、读取原始日志数据
    val actionsRDD = sc.textFile("datas/user_visit_action.txt")
    // 去掉shuffle操作，使用其他方式：累加器

    // 创建累加器
    val hc = new HotCategoryAccumulator()
    // 注册到spark
    sc.register(hc, "HotCategory")


    // 2、将数据转换结构
    actionsRDD.foreach(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          // 点击的场合
          hc.add((datas(6),"click"))
        } else if (datas(8) != "null") {
          // 下单的场合
          val cids = datas(8).split(",")
          cids.foreach(
            id => {
              hc.add((id,"order"))
            }
          )
        } else if (datas(10) != "null") {
          // 支付的场合
          val cids = datas(10).split(",")
          cids.foreach(
            id => {
              hc.add((id,"pay"))
            }
          )
        } else {
          // 返回空
          Nil
        }
      }
    )

    // 返回累加器的结果
    val hcVal = hc.value

    // 降序排序，取前10名
    val categories = hcVal.map(_._2)
    val result = categories.toList.sortWith(
      (left, right) => {
        if (left.clickCnt > right.clickCnt) {
          true
        } else if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.orderCnt) {
            true
          } else if (left.orderCnt == right.orderCnt) {
            left.payCnt > right.payCnt
          } else {
            false
          }
        } else {
          false
        }
      }
    ).take(10)

    // 5、将结果采集到控制台打印出来
    result.foreach(println)
    sc.stop()
  }

  // 使用样例类来封装数据 （cid , 点击数量, 下单数量， 支付数量）
  // 注意：点击数量, 下单数量， 支付数量 是可变的属性，所以需要用 var 修饰
  case class HotCategory(cid:String,var clickCnt:Int,var orderCnt:Int,var payCnt : Int)

  /**
   * 自定义累加器
   * 1、继承AccumulatorV2 定义泛型
   *    IN：（品类ID，行为类型）
   *    OUT：mutable.Map[String,HotCategory]
   * 2、重写方法（6个）
   *
   * */

  class HotCategoryAccumulator extends AccumulatorV2[(String,String),mutable.Map[String,HotCategory]]{
    private val hcMap = mutable.Map[String,HotCategory]()
    override def isZero: Boolean = {
      hcMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator
    }

    override def reset(): Unit = {
      hcMap.clear()
    }

    // 添加的参数为 IN 的类型
    override def add(v: (String, String)): Unit = {
      val cid = v._1
      val actionType = v._2
      val category = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if (actionType == "click"){
        category.clickCnt += 1
      }else if (actionType == "order"){
        category.orderCnt += 1
      }else if (actionType == "pay"){
        category.payCnt += 1
      }
      hcMap.update(cid, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1 = this.hcMap
      val map2 = other.value
      map2.foreach{
        case (cid, hc) => {
          val category = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          map1.update(cid, category)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = hcMap
  }

}
