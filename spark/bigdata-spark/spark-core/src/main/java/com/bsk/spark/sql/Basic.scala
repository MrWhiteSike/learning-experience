package com.bsk.spark.sql




import org.apache.commons.codec.Encoder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Basic {

  def main(args: Array[String]): Unit = {
    // TODO 创建Spark SQL的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // TODO 执行逻辑操作

    // DataFrame
//    val df = spark.read.json("datas/user.json")
//    df.show()
    // DataFrame => SQL : 直接写SQL语句的方式，需要创建临时表
//    df.createOrReplaceTempView("user")
//    spark.sql("select * from user").show()
//    spark.sql("select age,username from user").show()
//    spark.sql("select avg(age) from user").show()

    // DataFrame => DSL：不需要创建临时表，提供了一些方法来表达
    // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则: 即隐式类型转换
    import spark.implicits._
//    df.select("age","username").show()
    // 使用 $"" 方式来隐式转换
//    df.select($"age" > 30).show()
    // 单引号代替 $ : 为了代码看起来不乱
//    df.select('age > 30).show()



    // DataSet
    /*val seq = Seq(1,2,3,4)
    // 也是需要隐式转换的，必须在创建SparkSession的对象spark后添加 import spark.implicits._
    val ds = seq.toDS()
    ds.show()*/


    // RDD <=> DataFrame
    // Person(1,"zhangsan"), Person(2,"lisi"), Person(3,"wangwu")
    val rdd = spark.sparkContext.makeRDD(List((1, "zhangsan"), (2, "lisi"), (3, "wangwu")))
    val df = rdd.toDF("age", "name")
    df.show()
//    val rdd1 = df.rdd
//    println(rdd1.take(3).mkString(","))


    // DataFrame <=> DataSet
//    val ds = df.alias("user")
    // 以某个样例类作为数据结构
    val ds = df.as[Person]
    val df1 = ds.toDF()

    // RDD <=> DataSet
    // 先转换为样例类的数据结构，然后再转成DataSet
    val ds1 = rdd.map {
      case (age, name) => {
        Person(age, name)
      }
    }.toDS()
    val personRDD = ds1.rdd



    // TODO 关闭环境
    spark.close()

  }

  case class Person(age:Int,name:String)
}
