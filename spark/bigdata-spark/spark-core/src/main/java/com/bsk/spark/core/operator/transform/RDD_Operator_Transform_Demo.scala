package com.bsk.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_Demo {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))
    // 案例实操 - 求每个省份每个广告的top3

    // 1、获取原始数据： 时间戳，省份，城市，用户，广告
    val dataRDD = sc.textFile("datas/agent.log")

    // 2、将原始数据进行结构转换，方便统计
    // 时间戳，省份，城市，用户，广告 => （（省份，广告），1）
    val mapRDD = dataRDD.map(
      line => {
        val datas = line.split(" ")
        ((datas(1), datas(4)), 1)
      }
    )

    // 3、将转换后的数据，进行分组聚合
    // （（省份，广告），1）=> （（省份，广告），sum）
    val reduceRDD = mapRDD.reduceByKey(_ + _)

    // 4、将聚合的结果进行结构的转换
    // （（省份，广告），sum） => （省份，（广告，sum））
    val newMapRDD = reduceRDD.map {//  case 模式匹配
      case ((prv, ad), sum) => (prv, (ad, sum))
    }

    // 5、将转换后的数据进行分组
    // （省份，（广告，sum）） => （省份，【（广告A，sumA），（广告B，sumB），...】）
    val groupRDD = newMapRDD.groupByKey()

    // 6、将分组后的数据进行排序（降序），取前3名
    val resultRDD = groupRDD.mapValues( // 当key不变，只对value进行操作时使用mapValues
      iter => {
        // Iterator 是不可以排序的，转换为List就可以进行排序了；List 是scala的功能
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )

    // 7、将结果数据采集打印到控制台
    resultRDD.collect().foreach(println)
    sc.stop()
  }

}
