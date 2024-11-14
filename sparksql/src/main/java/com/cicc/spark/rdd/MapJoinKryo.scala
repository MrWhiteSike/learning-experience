package com.cicc.spark.rdd

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}

object MapJoinKryo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("sparkwordcountsort").setMaster("local[*]")
    // 方式1：全局默认，开启KryoSerializer序列化
    conf.set("spark.serializer", classOf[KryoSerializer].getName)

    // 方式2：手动注册，一般情况下不手动设置

    val sc = new SparkContext(conf)

    // Kyro可用于广播变量以及shuffle时候传输的对象，但是不能用于spark算子function中的外部对象，如果外部对象没有被Kryo进行序列化
    // 因为spark传给task的代码必须是可serializable的，这个外部对象可以通过Serializable进行序列化

  }
}
