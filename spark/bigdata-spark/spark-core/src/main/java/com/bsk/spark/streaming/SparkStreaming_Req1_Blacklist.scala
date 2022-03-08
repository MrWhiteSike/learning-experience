package com.bsk.spark.streaming

import java.text.SimpleDateFormat
import com.bsk.spark.utils.JDBCUtil
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer


object SparkStreaming_Req1_Blacklist {

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
        rdd.foreach{
          case ((day, user, ad), count) => {
            println(s"${day} ${user} ${ad} ${count}")
            if(count >= 30){
              // TODO 如果统计数量超过点击阈值(30)，那么将用户拉入到黑名单
              val connection = JDBCUtil.getConnection
              val ps = connection.prepareStatement(
                """
                  | insert into black_list (userid) values (?)
                  | on duplicate key
                  | update userid = ?
                  |""".stripMargin)
              ps.setString(1,user)
              ps.setString(2,user)
              ps.executeUpdate()
              ps.close()
              connection.close()
            }else{
              // TODO 如果没有超过阈值，那么需要将当天的广告点击数量进行更新。
              val connection = JDBCUtil.getConnection
              val ps = connection.prepareStatement(
                """
                  | select
                  |   *
                  | from user_ad_count
                  | where dt = ? and userid = ? and adid = ?
                  |""".stripMargin)

              ps.setString(1,day)
              ps.setString(2,user)
              ps.setString(3,ad)
              val rs = ps.executeQuery()
              // 查询统计表数据
              if (rs.next()){
                // 如果存在，那么更新
                val ps1 = connection.prepareStatement(
                  """
                    | update user_ad_count
                    | set count = count + ?
                    | where dt = ? and userid = ? and adid = ?
                    |""".stripMargin)
                ps1.setInt(1, count)
                ps1.setString(2, day)
                ps1.setString(3, user)
                ps1.setString(4, ad)
                ps1.executeUpdate()
                ps1.close()
                // TODO 判断更新后的点击数据是否超过阈值，如果超过，那么将用户拉入黑名单
                val ps2 = connection.prepareStatement(
                  """
                    | select
                    |   *
                    | from user_ad_count
                    | where dt = ? and userid = ? and adid = ? and count >= 30
                    |""".stripMargin)
                ps2.setString(1, day)
                ps2.setString(2, user)
                ps2.setString(3, ad)
                val rs2 = ps2.executeQuery()
                if (rs2.next()){
                  val ps3 = connection.prepareStatement(
                    """
                      | insert into black_list (userid) values (?)
                      | on duplicate key
                      | update userid = ?
                      |""".stripMargin)
                  ps3.setString(1,user)
                  ps3.setString(2,user)
                  ps3.executeUpdate()
                  ps3.close()
                }

                rs2.close()
                ps2.close()
              }else {
                // 如果不存在，那么新增
                val ps1 = connection.prepareStatement(
                  """
                    | insert into user_ad_count (dt,userid,adid,count) values (?,?,?,?)
                    |""".stripMargin)
                ps1.setString(1, day)
                ps1.setString(2, user)
                ps1.setString(3, ad)
                ps1.setInt(4, count)
                ps1.executeUpdate()
                ps1.close()
              }
              rs.close()
              ps.close()
              connection.close()
            }
          }
        }
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

  // 广告点击数据
  case class AdClickData(ts:String, area:String, city:String, user:String, ad:String)

}
