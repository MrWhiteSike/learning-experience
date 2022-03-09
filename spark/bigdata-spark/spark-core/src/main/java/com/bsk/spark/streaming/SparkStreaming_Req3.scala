package com.bsk.spark.streaming

import java.io.{File, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import com.bsk.spark.utils.JDBCUtil
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer


object SparkStreaming_Req3 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

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

    // 最近一分钟中，每10秒计算一次（）

    // 这里涉及窗口的计算：在某个窗口范围内，每个滑动时间窗口内的数据进行统计
    val ds = adClickData.map(
      data => {
        val ts = data.ts.toLong
        val newTs = ts / 10000 * 10000
        (newTs, 1)
      }
    ).reduceByKeyAndWindow((x:Int,y:Int) => {
      x + y
    }, Seconds(60), Seconds(10))

    // 打印
//    ds.print()

    // 时间排序后打印
    ds.foreachRDD(
      rdd => println(rdd.sortByKey(true).collect().mkString(","))
    )

    // 以一定格式输出到json文件中 TODO 出现重复的时间问题
    /*ds.foreachRDD(
      rdd => {
        val list = ListBuffer[String]()
        val datas = rdd.sortByKey(true).collect()
        datas.foreach{
          case (time,cnt) => {
            val stringTime = new SimpleDateFormat("hh:mm").format(time.toLong)
            list.append(s"""{"xtime":"${stringTime}","yval":"${cnt}"}""")
          }
        }

        // 输出文件
        val out = new PrintWriter(new FileWriter(new File("/Users/baisike/learning-experience/spark/bigdata-spark/datas/adclick.json")))
        out.println("["+list.mkString(",")+"]")
        out.flush()
        out.close()
      }
    )*/

    ssc.start()
    ssc.awaitTermination()
  }
  // 广告点击数据
  case class AdClickData(ts:String, area:String, city:String, user:String, ad:String)

}
