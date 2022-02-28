package com.bsk.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Action_06_Foreach_Demo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List[Int]())
    val user = new User()

    // org.apache.spark.SparkException: Task not serializable
    // Caused by: java.io.NotSerializableException: com.bsk.spark.core.operator.action.RDD_Operator_Action_06_Foreach_Demo$User


    // RDD 算子中传递的函数是会包含 闭包 操作，那么就会进行检测功能

    // 闭包：算子把外部变量引入到内部，形成一个闭合的效果，改变这个变量的生命周期
    // 检测：检测外部变量是不是序列化的，不需要去执行就可以判断出来

    // ClosureCleaner$.ensureSerializable 确保序列化的功能，还没到runJob
    rdd.foreach(
      num => {
        println("age = " + (user.age + num))
      }
    )

    sc.stop()
  }

  // 解决序列化问题的方法：
  //    1、extends Serializable 2、样例类，比如case class User()
//  class User extends Serializable {
  // 样例类在编译时，会自动混入序列特质
  // case class User() {
  class User {
    val age : Int = 30
  }

}
