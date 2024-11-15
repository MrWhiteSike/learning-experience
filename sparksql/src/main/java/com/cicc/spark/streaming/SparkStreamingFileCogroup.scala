package com.cicc.spark.streaming

import java.util.regex.Pattern

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 *
 * Cogroup : 不想停止程序就能达到更新某个配置的目的，就可以通过 这种方式 去实现
 *
 * 缺点：join的时候会产生宽依赖，经过网络传输，就会产生shuffle
 *
 * 停止程序更新配置就不是流式计算了
 *
 */
object SparkStreamingFileCogroup {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkstreamingfilecogroup").setMaster("local[1]")
    conf.set("spark.streaming.fileStream.minRememberDuration", "2592000s")
    val streamingContext = new StreamingContext(conf, Durations.seconds(5))

    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 6666)
    val flatMap: DStream[String] = lines.flatMap(_.split(" "))
    val mapToPair: DStream[(String, Int)] = flatMap.map((_, 1))
    val countryCount: DStream[(String, Int)] = mapToPair.reduceByKey(_ + _)

    val filePath: String = "C:\\Users\\Baisike\\project\\sparksql\\src\\main\\resources\\file"
    val hadoopConf: Configuration = new Configuration
    val fileDStream: InputDStream[(LongWritable, Text)] = streamingContext.fileStream[LongWritable, Text, TextInputFormat](filePath,
      (path: Path) => {
        println(path)
        path.toString.endsWith(".txt")
      }, false, hadoopConf)

//    fileDStream.map((a:(LongWritable, Text)) => {
//      // 这里可以进行优化： 使用mapPartitions， 每个分区创建一个Pattern对象, 就可以少创建Pattern对象
        // new Pattern("\t", 0).split("cm\t中国", 0)
//      val strings: Array[String] = a._2.toString.split("\t")
//      (strings(0),strings(1))
//    })

    // 优化方式实现
    val countryDictFile: DStream[(String, String)] = fileDStream.mapPartitions(iter => {
      val pattern = new Pattern("\t", 0)
      val list: List[(LongWritable, Text)] = iter.toList
      val tuples = new ListBuffer[(String, String)]()

      for (a <- list) {
        val strings: Array[String] = pattern.split(a._2.toString, 0)
        tuples += ((strings(0), strings(1)))
      }
      tuples.iterator
    })

    // 这个相当于sql中的join只有在两个流都有相同的key的时候，才会打印结果
//    countryCount.join(countryDictFile).foreachRDD( (r,t) => {
//      println(s"time:${t}")
//      r.foreach(f => {
//        val code: String = f._1
//        val count: Int = f._2._1
//        val name: String = f._2._2
//
//        println(s"code:${code},count:${count},name:${name}")
//
//      })

    // 这个相当于sql中的full join ，两个都有相同的key的时候，会进行join，join不上的也会进入这个函数
    countryCount.cogroup(countryDictFile).foreachRDD( (r,t) => {
      println(s"time:${t}")
      r.foreach(f => {
        val code: String = f._1
        val count: Iterable[Int] = f._2._1
        val name: Iterable[String] = f._2._2

        println(s"code:${code},count:${count},name:${name}")

      })

    })

    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
