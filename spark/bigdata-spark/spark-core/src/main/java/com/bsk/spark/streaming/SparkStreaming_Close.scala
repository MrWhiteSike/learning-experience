package com.bsk.spark.streaming

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}


object SparkStreaming_Close {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val lines = ssc.socketTextStream("localhost", 9999)
    val wordToOne = lines.map((_, 1))

    wordToOne.print()
    ssc.start()

    // 如果想要关闭采集器，那么需要创建新的线程
    // 而且需要在第三方程序中增加关闭状态
    new Thread(
      new Runnable {
        override def run(): Unit = {
          // 优雅的关闭, 第二个参数就是优雅的关闭：
          // 计算节点不在接收新的数据，而是将现有的数据处理完毕，然后关闭
          // MySQL : Table ==> row => data
          // Redis : Data(k-v)
          // zk: /stopSpark
          // HDFS : /stopSpark

          // 真实场景中使用
          // 获取 HDFS 文件系统的连接
//          val fs =  FileSystem.get(new URI("hdfs://localhost:9090"),new Configuration(),"test")
          /*while (true){
            // 判断第三方程序中是否出现需要关闭SparkStreaming的状态
            val thirdState = fs.exists(new Path("hdfs://localhost:9090/stopSpark"))
            if (thirdState){
              // 获取SparkStreaming的状态
              val state = ssc.getState()
              if (state == StreamingContextState.ACTIVE){
                ssc.stop(true, true)
                // 停止当前线程
                System.exit(0)
              }
            }
            try
              Thread.sleep(5000)
            catch {
              case e: InterruptedException =>
                e.printStackTrace()
            }
          }*/

          // 测试使用
          Thread.sleep(5000)
          val state = ssc.getState()
          if (state == StreamingContextState.ACTIVE){
            ssc.stop(true, true)
            System.exit(0)
          }
        }
      }
    ).start()

    ssc.awaitTermination() // block 阻塞main线程
  }

}
