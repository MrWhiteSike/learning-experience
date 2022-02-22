package com.bsk.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Rdd_File_Par {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Rdd_File"))
    // textFile : 可以将文件作为数据处理的数据源，默认也可以设定分区。
    //      minPartitions ： 最小分区数量
    //      math.min(defaultParallelism, 2) 默认 2 个分区
//    val rdd = sc.textFile(path = "datas/1.txt")

    // 如果不想使用默认的分区数量，可以通过第二个参数指定分区数
    // 注意：Spark 读取文件，底层其实使用的是Hadoop的读取方式
    // 分区数量的计算方式：
    //      totalSize = 23
    //      goalSize = 23 / 3 = 7 (byte)
    //      23 / 7 = 3.2..（>10%）+ 1 = 4 (分区) (Hadoop 读取文件时，有个1.1 倍概念：如果剩余的字节数/每个分区字节数 > 10%时，产生一个新分区；如果 < 10%，不会产生新的分区)

    // 分区数据的分配：
    // 1、数据以行为单位进行读取，和字节数没有关系
    // 2、数据读取时是以偏移量为单位，偏移量不会被重复读取。
    // 3、数据分区的偏移量范围的计算：
    //    goalSize = 23 / 3 = 7 (byte)
    //    [0,7] 不够一行的，读取整行；
    //    [7,14] 由于不能重复读取，开始读取下一行，但是数据也是不够一行，也是读取整行；数据读取完了
    //    [14,21] 该分区没有数据
    //    [21,23] 该分区没有数据

    // 如果数据源为多个文件，那么计算分区时以文件为单位进行分区
    val rdd = sc.textFile(path = "datas/1.txt", 3)
    rdd.saveAsTextFile("output")
    sc.stop()
  }

}
