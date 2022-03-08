package com.bsk.spark.streaming


import java.text.SimpleDateFormat
import java.util.Date

import com.bsk.spark.utils.JDBCUtil
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming_Req2 {

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
    val ds = adClickData.map(
      data => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val day = sdf.format(new Date(data.ts.toLong))
        val area = data.area
        val city = data.city
        val ad = data.ad
        ((day, area, city, ad), 1)
      }
    ).reduceByKey(_+_)

    ds.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            val connection = JDBCUtil.getConnection
            val ps = connection.prepareStatement(
              """
                | insert into area_city_ad_count (dt,area,city,adid,count)
                | values (?,?,?,?,?)
                | on DUPLICATE KEY
                | UPDATE count = count + ?
                |""".stripMargin)
            iter.foreach{
              case ((day, area, city, ad), count) => {
                ps.setString(1,day)
                ps.setString(2,area)
                ps.setString(3,city)
                ps.setString(4,ad)
                ps.setInt(5, count)
                ps.setInt(6, count)
                ps.executeUpdate()
              }
            }
            ps.close()
            connection.close()
          }
        )
      }
    )


    ssc.start()
    ssc.awaitTermination()
  }
  // 广告点击数据
  case class AdClickData(ts:String, area:String, city:String, user:String, ad:String)

}
