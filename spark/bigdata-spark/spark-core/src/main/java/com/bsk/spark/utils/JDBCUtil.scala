package com.bsk.spark.utils

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

object JDBCUtil {
  // 初始化连接池
  var dataSource: DataSource = init()

  // 初始化连接池方法
  def init(): DataSource = {
    val properties = new Properties()
    properties.setProperty("driverClassName", "com.mysql.cj.jdbc.Driver")
    properties.setProperty("url","jdbc:mysql://localhost:3306/spark-test")
    properties.setProperty("username","root")
    properties.setProperty("password","123456")
    properties.setProperty("maxActive","50")
    DruidDataSourceFactory.createDataSource(properties)
  }

  // 获取MySQL连接
  def getConnection: Connection = {
    dataSource.getConnection
  }

  // 执行SQL语句，单条数据插入
  def executeUpdate(connection: Connection, sql:String, params:Array[Any]): Int = {
    var rtn = 0
    var ps : PreparedStatement = null
    try {
      connection.setAutoCommit(false)
      ps = connection.prepareStatement(sql)
      if (params != null && params.length > 0){
        for (i <- params.indices){
          ps.setObject(i+1, params(i))
        }
      }
      rtn = ps.executeUpdate()
      connection.commit()
      ps.close()
    } catch {
      case e : Exception => e.printStackTrace()
    }
    rtn
  }

  // 判断一条数据是否存在
  def isExist(connection: Connection, sql:String, params:Array[Any]) = {
    var flag = false
    var ps:PreparedStatement = null
    try {
      ps = connection.prepareStatement(sql)
      for(i <- params.indices){
        ps.setObject(i+1, params(i))
      }
      flag = ps.executeQuery().next()
      ps.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    flag
  }

  def main(args: Array[String]): Unit = {
    val connection = JDBCUtil.getConnection
    val ps = connection.prepareStatement("select * from black_list")
    val rs = ps.executeQuery()
    if (rs.next()){
      println(rs.getString(1))
    }
    println("ok")
  }
}
