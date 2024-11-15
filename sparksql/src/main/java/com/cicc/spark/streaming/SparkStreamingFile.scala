package com.cicc.spark.streaming

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingFile {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("sparkstreamingfile").setMaster("local[1]")
    conf.set("spark.streaming.fileStream.minRememberDuration", "2592000s")
    val streamingContext = new StreamingContext(conf, Durations.seconds(5))

    val filePath: String = "C:\\Users\\Baisike\\project\\sparksql\\src\\main\\resources\\file"

    val hadoopConf: Configuration = new Configuration

    val fileDStream: InputDStream[(LongWritable, Text)] = streamingContext.fileStream[LongWritable, Text, TextInputFormat](filePath,
      (path: Path) => {
        println(path)
        path.toString.endsWith(".txt")
      }, false, hadoopConf)

    val flatMap: DStream[String] = fileDStream.flatMap(_._2.toString.split(" "))
    val map: DStream[(String, Int)] = flatMap.map((_, 1))
    val reduceByKey: DStream[(String, Int)] = map.reduceByKey(_ + _)

    reduceByKey.foreachRDD((r, t) => {
      println(s"count time:${t}, ${r.collect().toList}")
    })


    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
