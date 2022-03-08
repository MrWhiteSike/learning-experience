package com.bsk.spark.streaming

import java.text.SimpleDateFormat

import com.bsk.spark.utils.JDBCUtil
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer


object SparkStreaming_Req1_Blacklist1 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val kafkaPara = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "test",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDataDStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("test"), kafkaPara)
    )
    val adClickData = kafkaDataDStream.map(
      kafkaData => {
        val data = kafkaData.value()
        val datas = data.split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    val ds = adClickData.transform(
      rdd => {
        // TODO 通过JDBC周期性获取黑名单数据
        val blackList = ListBuffer[String]()

        val connection = JDBCUtil.getConnection
        val ps = connection.prepareStatement("select userid from black_list")
        val rs = ps.executeQuery()
        while (rs.next()) {
          blackList.append(rs.getString(1))
        }

        rs.close()
        ps.close()
        connection.close()

        // TODO 判断点击用户是否在黑名单中
        val filterRDD = rdd.filter(
          data => {
            !blackList.contains(data.user)
          }
        )

        // TODO 如果用户不在黑名单中，那么进行统计数量（一个采集周期）
        filterRDD.map(
          data => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val day = sdf.format(new java.util.Date(data.ts.toLong))
            val user = data.user
            val ad = data.ad
            ((day, user, ad), 1)
          }
        ).reduceByKey(_+_)
      }
    )

    ds.foreachRDD(
      rdd => {
        // 问题：rdd.foreach 方法会每一条数据创建连接

        // foreach是RDD的算子，算子之外的代码是在Driver端执行，算子内的代码是在Executor端执行的
        // 这样就会涉及到闭包操作，Driver端的数据就需要传递到 Executor 端，需要将数据进行序列化
        // 但是，数据库的连接对象是不能序列化的

        // RDD 提供了一个算子可以有效提升效率 : foreachPartition
        // 可以一个分区创建一个连接对象，可以大幅度减少连接对象的数量，提升效率
        rdd.foreachPartition(
          iter => {
            // 分区中创建 数据库连接
            val connection = JDBCUtil.getConnection
            iter.foreach{
              case ((day, user, ad), count) => {
                println(s"${day} ${user} ${ad} ${count}")
                if(count >= 30){
                  // TODO 如果统计数量超过点击阈值(30)，那么将用户拉入到黑名单
                  val sql = """
                              | insert into black_list (userid) values (?)
                              | on duplicate key
                              | update userid = ?
                              |""".stripMargin
                  // 相同的代码进行封装，减少重复代码
                  JDBCUtil.executeUpdate(connection, sql, Array(user,user))
                }else{
                  // TODO 如果没有超过阈值，那么需要将当天的广告点击数量进行更新。
                  val sql = """
                              | select
                              |   *
                              | from user_ad_count
                              | where dt = ? and userid = ? and adid = ?
                              |""".stripMargin
                  val flag = JDBCUtil.isExist(connection,sql, Array(day,user,ad))
                  // 查询统计表数据
                  if (flag){
                    // 如果存在，那么更新
                    val sql1 = """
                                 | update user_ad_count
                                 | set count = count + ?
                                 | where dt = ? and userid = ? and adid = ?
                                 |""".stripMargin
                    JDBCUtil.executeUpdate(connection, sql1, Array(count, day, user, ad))
                    // TODO 判断更新后的点击数据是否超过阈值，如果超过，那么将用户拉入黑名单
                    val sql2 = """
                                 | select
                                 |   *
                                 | from user_ad_count
                                 | where dt = ? and userid = ? and adid = ? and count >= 30
                                 |""".stripMargin
                    val flag2 = JDBCUtil.isExist(connection, sql2, Array(day, user, ad))
                    if (flag2){
                      val sql3 = """
                                   | insert into black_list (userid) values (?)
                                   | on duplicate key
                                   | update userid = ?
                                   |""".stripMargin
                      JDBCUtil.executeUpdate(connection, sql3, Array(user,user))
                    }
                  }else {
                    // 如果不存在，那么新增
                    val sql4 = """
                                 | insert into user_ad_count (dt,userid,adid,count) values (?,?,?,?)
                                 |""".stripMargin
                    JDBCUtil.executeUpdate(connection, sql4, Array(day, user, ad, count))
                  }
                }
              }
            }
            connection.close()
          }
        )


        /*rdd.foreach{
          case ((day, user, ad), count) => {
            println(s"${day} ${user} ${ad} ${count}")
            if(count >= 30){
              // TODO 如果统计数量超过点击阈值(30)，那么将用户拉入到黑名单
              val connection = JDBCUtil.getConnection
              val sql = """
                          | insert into black_list (userid) values (?)
                          | on duplicate key
                          | update userid = ?
                          |""".stripMargin
              // 相同的代码进行封装，减少重复代码
              JDBCUtil.executeUpdate(connection, sql, Array(user,user))
              connection.close()
            }else{
              // TODO 如果没有超过阈值，那么需要将当天的广告点击数量进行更新。
              val connection = JDBCUtil.getConnection
              val sql = """
                          | select
                          |   *
                          | from user_ad_count
                          | where dt = ? and userid = ? and adid = ?
                          |""".stripMargin
              val flag = JDBCUtil.isExist(connection,sql, Array(day,user,ad))
              // 查询统计表数据
              if (flag){
                // 如果存在，那么更新
                val sql1 = """
                             | update user_ad_count
                             | set count = count + ?
                             | where dt = ? and userid = ? and adid = ?
                             |""".stripMargin
                JDBCUtil.executeUpdate(connection, sql1, Array(count, day, user, ad))
                // TODO 判断更新后的点击数据是否超过阈值，如果超过，那么将用户拉入黑名单
                val sql2 = """
                             | select
                             |   *
                             | from user_ad_count
                             | where dt = ? and userid = ? and adid = ? and count >= 30
                             |""".stripMargin
                val flag2 = JDBCUtil.isExist(connection, sql2, Array(day, user, ad))
                if (flag2){
                  val sql3 = """
                               | insert into black_list (userid) values (?)
                               | on duplicate key
                               | update userid = ?
                               |""".stripMargin
                  JDBCUtil.executeUpdate(connection, sql3, Array(user,user))
                }
              }else {
                // 如果不存在，那么新增
                val sql4 = """
                             | insert into user_ad_count (dt,userid,adid,count) values (?,?,?,?)
                             |""".stripMargin
                JDBCUtil.executeUpdate(connection, sql4, Array(day, user, ad, count))
              }
              connection.close()
            }
          }
        }*/
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }

  // 广告点击数据
  case class AdClickData(ts:String, area:String, city:String, user:String, ad:String)

}
