package com.bsk.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Serial_01 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("serial")
    val sc = new SparkContext(conf)

    val search = new Search("h")

    // SparkException: Task not serializable
    // java.io.NotSerializableException: com.bsk.spark.core.operator.serial.Serial_01$Search
    val rdd = sc.makeRDD(Array("hello scala", "hello spark", "hive", "atguigu"))
    // search.getMatch1(rdd).collect().foreach(println)
    search.getMatch2(rdd).collect().foreach(println)

    sc.stop()
  }

  // 注意：
  // 1、在scala 语法当中，类的构造参数其实是类的属性，构造参数需要进行闭包检测，其实就等同于类闭包检测；类混入Serializable特质 来解决序列化问题
  // 2、使用case 样例类 来解决
  // 3、在方法中添加val s = query，s用到rdd算子中，就可以解决序列化问题；因为 s 是方法的局部变量，和类没有关系，并且 s 是String类型，是可以序列化的，所以类不用实现序列化，这种方式也可以解决序列化问题
  class Search(query: String) {
    def isMatch (s: String): Boolean ={
      s.contains(query) // ==> s.contains(this.query)，类的对象属性
    }

    // 函数序列化案例
    def getMatch1(rdd: RDD[String]): RDD[String] ={
      rdd.filter(isMatch)
    }

    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      val s = query
      rdd.filter(x => x.contains(s))
    }

  }

}
