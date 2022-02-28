package com.bsk.spark.core.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}

object Checkpoint_04 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Test")
    val sc = new SparkContext(conf)
    // 设置 checkpoint 路径
    sc.setCheckpointDir("cp")

    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map(
        word => {
          println(" #########")
          (word,1)
        }
      )
    /*
 cache：将数据临时存储在内存中进行数据重用
 			会在血缘关系中添加新的依赖,一旦出现问题，可以重头读取数据
 persist：将数据临时存储在磁盘文件中进行数据重用
 			涉及到磁盘IO，性能较低，但是数据安全
 			如果作业执行完毕，临时保存到数据文件就会丢失
 			会在血缘关系中添加新的依赖,一旦出现问题，可以重头读取数据
 checkpoint：将数据长久保存在磁盘文件中进行数据重用
 			涉及到磁盘IO，性能较低，但是数据安全
 			为了保证数据安全，所以一般情况下，会独立执行作业（另外再执行一次）。
 			为了能够提高效率，一般情况下，是需要我们和cache联合使用的
 			执行过程中，会切断血缘关系，重新建立新的血缘关系
 			checkpoint等同于改变数据源
    * */


//    mapRDD.cache()
    mapRDD.checkpoint()

    //
    println(mapRDD.toDebugString)
    val reduceRDD = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("*******************************")
    println(reduceRDD.toDebugString)


    sc.stop()
  }

}
