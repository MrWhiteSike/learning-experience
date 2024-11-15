package com.cicc.spark.streaming

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
 * 考虑的问题是可能会产生很多小文件，小文件带来的问题是什么？
 *
 * 占用很大的namenode的元数据空间，下游使用小文件的job会产生很多个partition，如果是mr任务就会有很多个map任务，如果是spark任务就会有很多task，那如何解决？
 *
 * 可以使用的方法有4种：
 * 1. 增加batch大小
 * 2. 使用批处理任务进行小文件的合并
 * 3. 使用coalesce
 * 4. 使用HDFS的append方式
 *
 * 1和2是不建议使用的
 *
 *
 */
object SparkStreamingHdfs {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkstreaminghdfs").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Durations.seconds(5))
    val lines = streamingContext.socketTextStream("localhost", 6666)

    val flatMap: DStream[String] = lines.flatMap(_.split(" "))
    val map: DStream[(String, Int)] = flatMap.map((_, 1))
    val reduceByKey: DStream[(String, Int)] = map.reduceByKey(_ + _)

    reduceByKey.foreachRDD(r => {
      if (!r.isEmpty()){
        // 可以使用coalesce(1)减少输出的文件数， 但是不建议设置为1，因为如果为1就把并行计算变成单线程了， 实际应用中应该和你任务的cpu cores 保持一致
        // 这里为什么使用 mapPartitionsWithIndex 呢？ 因为可以每个partition的task拿到自己的partitionid，从而让partitionID拼接到生成输出文件的文件名上，以防止各个task之间的冲突
        val rdd: RDD[String] = r.coalesce(1).mapPartitionsWithIndex((partitionId, f) => {
          // 创建 FileSystem 对象
          val conf = new Configuration()
          val fs: FileSystem = FileSystem.get(conf)

          // 获取每个partition中的数据
          val list: List[(String, Int)] = f.toList
          if (list.length > 0) {
            // 这里为什么生成每小时的字符串呢？ 因为可以拼接到文件名上，以判断每小时的文件是否存在，如果存在就追加，不存在就创建
            val format: String = new SimpleDateFormat("yyyyMMddHH").format(new Date())

            // 注意：使用
            val path = new Path(s"C:\\Users\\Baisike\\project\\sparksql\\src\\main\\resources\\output\\sparkstreaminghdfs\\${partitionId}_${format}")

            // 如果存在就append流，如果不存在就创建文件并返回流
            // append 的方式只支持集群的hdfs，不支持local模式的hdfs
            // 所以如果local模式的hdfs的下一个批次写入的时候就会报错：not support append
            // 注意：使用集群的core-site.xml 和 hdfs-site.xml 才不会报错，需要把集群上这两个文件下载本地放到项目下的resources目录下
            val outPutStream: FSDataOutputStream = if (fs.exists(path)) {
              fs.append(path)
            } else {
              fs.create(path)
            }

            list.foreach(f => {
              outPutStream.write(s"${f._1}\t${f._2}\n".getBytes("UTF-8"))
            })

            outPutStream.close()
          }
          new ArrayBuffer[String]().toIterator
        })
        //这里为什么用一个action，因为刚才的 mapPartitionsWithIndex 是一个 transformation ，
        // 如果没有一个action 任务将不会执行，而刚才的所有逻辑都写到了 mapPartitionsWithIndex 的function中，
        // 所以必须启动一个action执行刚才的逻辑
        // foreach空转，是执行的，foreachPartitions 里面为空的话，是不执行的。
        rdd.foreach(_ => Unit)
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
