package com.cicc.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingSocketPortUpdateStateWindow {


  def main(args: Array[String]): Unit = {

//    val checkpoint = "C:\\Users\\Baisike\\project\\sparksql\\src\\main\\resources\\output\\sparkstreamingsocketportupdatestatewindow"

    val conf: SparkConf = new SparkConf().setAppName("sparkstreamingsocketportupdatestatewindow").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Durations.seconds(5))

    // 注意：使用 updateStateByKey 的时候，必须要设置一个checkpoint地址用于保存历史的数据
//    streamingContext.checkpoint(checkpoint)

    // 注意：在有checkpoint数据的时候， 修改地址和端口是不生效的
    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 6666)
    // 测试修改地址和端口是否生效
    //      val lines = streamingContext.socketTextStream("localhost", 7777)

    val flatMap: DStream[String] = lines.flatMap(_.split(" "))
    val mapToPair: DStream[(String, Int)] = flatMap.map((_, 1))
    // TODO 测试1：当没有设置滑动间隔的时候，那滑动间隔默认和批次间隔相同，
    //  这个window没有用到checkpoint保存数据，用的是内存； reduceByKey 聚合之后的数据量比较少，window 使用内存是没有问题的
    //  建议使用这种方式，因为计算过后的数据量少，内存撑得住，计算时间短，程序性能好
    val reduceByKey: DStream[(String, Int)] = mapToPair.reduceByKey(_ + _).window(Durations.seconds(20), Durations.seconds(10))

    // TODO 测试3：countByValueAndWindow 这种 window 必须设置 checkpoint 的地址， 是用磁盘存储window的数据， 并直接返回统计好的值。
    // 为啥使用磁盘存储window数据？因为进入window的是没有合并结果的，所以数据量会比较大，使用内存风险较高
    // 不建议使用这种方式，因为使用了磁盘，计算速度慢，程序性能不好
    val value: DStream[(String, Long)] = flatMap.countByValueAndWindow(Durations.seconds(20), Durations.seconds(10))

    // TODO 测试4：这种方式尽量避免使用！！！ 这个是使用了内存保存window的数据，不用设置checkpoint的地址，
    //  使用了DStream的countByValue， 代替了 reduceByKey，返回统计好的结果
    // 不建议使用这种方式，因为进入window的数据量大，内存有可能撑不住，计算时间长，程序性能不好
    val value1: DStream[((String, Int), Long)] = mapToPair.window(Durations.seconds(20), Durations.seconds(10)).countByValue()

    /**
     * 总结：
     * Spark Streaming 使用内存的方法有哪些，比如，DStream的 cache、persist，window, 还有streamingContext.remember
     * 另外，foreachRDD和transform中得到RDD，进行了cache，persist
     *
     * DStream的 cache 默认级别是 MEMORY_ONLY_SER，
     * RDD的 cache 默认级别是 MEMORY_ONLY
     * 因为Ser 可以减少缓存的大小，更利于流式计算程序的稳定，也就浪费点cup资源，节省点内存资源。
     *
     */

    /**
     * transform 和 foreachRDD ：
     * 都可以进行RDD转换，比如RDD转成DS或DF进行spark-sql操作，这时就方便多了，可以使用sql中的一些函数，比如 row_number等
     * 但是要注意 transform 是转换操作，foreachRDD 是 动作，这两个的共同特点是可以让写spark-streaming程序像写spark-core一样
     * 这两个算子中的代码运行的时候分为本地端和集群端运行的（比如算子中的函数）
     *
     * DStream和RDD有一样的filter map 等算子，并且自己独有的比如，updateStateByKey，window等这样的算子
     *
     * 但是DStream 转换的函数返回的另一个DStream，所以不像 transform 和 foreachRDD 的函数可以得到原始的RDD，
     * 所以普通的DStream 算子无法进行spark-sql的运算
     *
     *
     * 注意：目前来看，只有这两个transform 和 foreachRDD 算子里的函数体中的代码分为 ”本地“和”集群“ 运算的，
     * 而DStream的其他算子的函数体中的代码是不分本地和集群的，因为他们都是在集群端运行的
     *
     */

    // transform 的具体用法
//    lines.transform(r => {
//      // 1. 上面DSTream的操作，可以转换为下面RDD的操作
//      val flatMap: RDD[String] = r.flatMap(_.split(" "))
//      val mapToPair: RDD[(String, Int)] = flatMap.map((_, 1))
//      val reduceByKey: RDD[(String, Int)] = mapToPair.reduceByKey(_ + _)
//
//      // 2. 可以扩展使用spark-sql的
//      // 导入隐式转换, 转换为
////      import sparkSession.implicit._
////      val df:DataFrame = reduceByKey.toDF
////      df.createOrReplaceTempView("test")
////      val df1:DataFrame = sparkSession.sql("select * from text")
////      val rdd:RDD[Row] = df1.rdd
//
//      reduceByKey
//    })


    // TODO 测试2：当 使用updateStateByKey 并且 滑动间隔 小于 窗口间隔的计算数据的时候，保存状态的数据会重复计算两个窗口重叠的批次窗口的数据
    val updateStateByKey: DStream[(String, Int)] = reduceByKey.updateStateByKey((a: Seq[Int], b: Option[Int]) => {
      // 参数a：本批次的数据
      // 参数b：这个key上批次计算的历史结果
      var total = 0
      for (i <- a) {
        total += i
      }
      // 这里需要先判断一下，因为这个key有可能是第一次进入，也就是没有历史数据，那此时应该给个初始化值0
      val last: Int = if (b.isDefined) b.get else 0

      // 测试修改是否生效
      println(last)
      val now: Int = total + last
      Some(now)
    })

    updateStateByKey.foreachRDD((r, t) => {
      println(s"count time : ${t}, ${r.collect().toList}")
    })

    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
