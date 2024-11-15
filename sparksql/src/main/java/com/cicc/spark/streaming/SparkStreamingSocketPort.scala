package com.cicc.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingSocketPort {

  /**
   * DStream 操作
   *
   * DStream 上的原语与RDD类似，分为Transformation（转换）和 Output Operations（输出）两种，此外转换操作中还有一些比较特殊的原语
   * 如：updateStateByKey()、transform()以及各种Window相关的原语
   *
   * updateStateByKey：用于记录历史记录，通过保存每次数据处理的结果，记录和更新某个状态State，这个状态可以为任意类型，
   *                   比如Optional<Integer>；
   *                   注意：使用 updateStateByKey 的时候，必须要设置一个checkpoint地址用于保存历史的数据
   *
   *
   * transform：允许DStream上执行任意的 RDD - 转换为DF或DS - RDD函数，通过该函数可以方便的扩展Spark API，间接支持SparkSql的模块使用
   *
   * 1. DStream的 Transformation操作
   *
   * map：
   * flatMap
   * filter
   * repartition
   * union
   * count
   * reduce
   * countByValue
   * reduceByKey
   * join
   * cogroup
   * transform
   * updateStateByKey
   *
   * OldDStream -> NEWDStream
   *
   *
   *
   * 2. Window 窗口转换操作
   * 在Spark Streaming中，数据处理使按批进行的，而数据采集是逐条进行的，因此在Spark Streaming 中会先设置好批处理间隔（batch duration）
   * 当超过批处理间隔的时候就会把采集到的数据汇总起来成为一批数据交给系统去处理。
   *
   * 对于窗口操作而言，在其窗口内部会有N个批处理数据，批处理数据的大小由窗口间隔（window duration）决定，而窗口间隔指的是窗口的持续时间，
   * 在窗口操作中，只用窗口的长度满足了才会触发批数据的处理。除了窗口的长度，窗口操作还有一个重要的参数，就是滑动间隔（slide duration）,
   * 它指的是经过多长时间窗口滑动一次形成新的窗口，滑动窗口默认情况下和批次间隔的相同，而窗口间隔一般设置的要比他们两个大。
   * 注意：滑动间隔和窗口间隔的大小必须设置为批次间隔的整数倍。
   *
   * 窗口的计算，允许你通过滑动窗口对数据进行转换，窗口转换操作如下：
   *
   * window(windowDuration, slideDuration): 返回一个基于原DStream的窗口批次计算后得到新的DStream
   *
   * countByWindow(windowDuration, slideDuration)：返回基于滑动窗口的DStream中的元素数量
   *
   * reduceByWindow(reduceFunc, windowDuration, slideDuration): 基于滑动窗口对原DStream的元素进行聚合操作，得到新的DStream
   * reduceByWindow(reduceFunc, invReduceFunc, windowDuration, slideDuration)：
   * 一个更高效的reduceByWindow的实现版本，先对滑动窗口中新的时间间隔内数据增量聚合并移去最早的不在当前窗口间隔内的数据统计量。
   * 这种方法可以复用中间窗口重合的部分，提交统计的效率。
   *
   * reduceByKeyAndWindow(reduceFunc, windowDuration, slideDuration, numPartitions)：基于滑动窗口对（k, v）键值对类型的原DStream的元素按Key进行聚合操作，得到一个新的DStream
   * reduceByKeyAndWindow(reduceFunc, invReduceFunc, windowDuration, slideDuration, numPartitions)：
   * 一个更高效的reduceByKeyAndWindow的实现版本，先对滑动窗口中新的时间间隔内数据增量聚合并移去最早的不在当前窗口间隔内的数据统计量。
   * 这种方法可以复用中间窗口重合的部分，提交统计的效率。
   *
   * countByValueAndWindow(windowDuration, slideDuration, [numPartitions]): 基于滑动窗口计算DStream中每个RDD内每个元素出现的频次
   * 并返回DStream[(k,Long)] ，其中K是RDD中元素的类型。Long是元素频次。与countByValue一样，reduce任务的数量可以通过一个可选参数进行配置。
   *
   *
   *
   * 3. DStream 输出操作
   *
   * spark streaming运行DStream的数据被输出到外部系统，如数据库或文件系统。
   * 由于输出操作实际上使transformation操作后的数据可以通过外部系统被使用，同时输出操作触发所有DStream的transformation操作的实际执行。
   * 类似RDD的action的操作。
   *
   * print() : 在Driver中打印DStream中数据的前10个元素
   *
   * saveAsTextFiles(prefix, [suffix]):
   * 将DStream中的内容以文本的形式保存为文本文件，其中每批次处理间隔产生的文件以 prefix-TIME_IN_MS[_suffix]的方式命名
   *
   *
   * saveAsObjectFiles(prefix, [suffix])：
   * 将DStream中的内容按对象序列化并且以SequenceFile的格式保存，其中每批次处理间隔产生的文件以 prefix-TIME_IN_MS[_suffix]的方式命名
   *
   * saveAsHadoopFiles(prefix, [suffix])：
   * 将DStream中的内容以文本的形式保存为hadoop文件，其中每批次处理间隔产生的文件以 prefix-TIME_IN_MS[_suffix]的方式命名
   *
   *
   * foreachRDD(func):
   * 最基本的输出操作，将func函数应用于DStream中的RDD上，这个操作会输出数据到外部系统，比如保存RDD到文件或者网络数据库等
   * 需要注意的是func函数是在运行该streaming应用的Driver进程里执行的。
   * 而spark-core 中的 foreach 中的 func 是在Executor端执行的
   *
   */


  def main(args: Array[String]): Unit = {

    /**
     * 对于sparkStreaming程序至少有2个的cpu cores的资源，也就是说集群上的空闲资源必须大于1个cpu cores，
     * 因为receiver 会占用一个cpu core，剩下的一个或多个cpu core是给处理数据的executor用的，
     * 所以你在使用spark-submit提交任务的时候设置cpu 资源参数一定大于1，对于本地测试来讲local[n]， n也一定大于1，
     * 否则就会提示资源不够，然后任务无法运行。
     */
    val conf = new SparkConf().setAppName("sparkstreamingsocketport").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Durations.seconds(5))
    val lines = streamingContext.socketTextStream("localhost", 6666)



    // 实现方式1：
    val flatMap = lines.flatMap(_.split(" "))
    val mapToPair = flatMap.map((_, 1))
    val reduceByKey: DStream[(String, Int)] = mapToPair.reduceByKey(_ + _)

    /**
     *
     * DStream 的数据输出与RDD的不同：
     *
     * foreachRDD(func):
     * 最基本的输出操作，将func函数应用于DStream中的RDD上，这个操作会输出数据到外部系统，比如保存RDD到文件或者网络数据库等
     * 需要注意的是func函数是在运行该streaming应用的Driver进程里执行的。
     * 而spark-core 中的 foreach 中的 func 是在Executor端执行的
     *
     */
    reduceByKey.foreachRDD((r,t) => {
      // r是RDD, t 是时间
      // 这个打印在Driver端进行输出的
      println(s"count time : ${t}, ${r.collect().toList}")

      // RDD算子的function是在集群端执行
      r.map(f => {
        // 这个地方才是在Executor端执行的操作
        f._1.split(" ")
      })
    })



    // 实现方式2：也可以只用foreachRDD的形式进行单独的RDD开发
    // 注意：foreachRDD函数中的打印在Driver端执行的， RDD算子的function是在集群端执行，transform 也是同样的用法！
//    lines.foreachRDD((r,t) => {
//      val flatMap = r.flatMap(_.split(" "))
//      val mapToPair = flatMap.map((_, 1))
//      val reduceByKey = mapToPair.reduceByKey(_ + _)
//      println(s"count time : ${t}, ${reduceByKey.collect().toList}")
//    })

    // 必须有下面两行代码，开启任务，阻塞任务然后不断的去处理流式数据
    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
