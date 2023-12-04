package com.yisa.engine.db

import java.sql.{ DriverManager, Connection }
import java.util
import com.yisa.wifi.zookeeper.ZookeeperUtil

/**
 *
 * @author liliwei
 * @date  2016年10月9日
 * mysql数据库连接池
 *  the connections in the pool should be lazily created on demand and timed out if not used for a while. 
 */
private[db] object MySQLConnectionPool {
  private val max = 3 //连接池连接总数
  private val connectionNum = 5 //每次产生连接数
  private var conNum = 0 //当前连接池已产生的连接数
  private val pool = new util.LinkedList[Connection]() //连接池

  //获取连接
  def getConnet(zkHostport: String): Connection = {
    //同步代码块
    AnyRef.synchronized({

      if (pool.isEmpty) {
        //加载驱动
        preGetConn()
        for (i <- 1 to connectionNum) {
          val zkUtil = new ZookeeperUtil()
          val configs = zkUtil.getAllConfig(zkHostport, "spark_engine", false)
          var jdbc = configs.get("spark_engine_jdbc")
          val URL = jdbc
          val USERNAME = configs.get("spark_engine_jdbc_username")
          val PASSWORD = configs.get("spark_engine_jdbc_password")
          val conn = DriverManager.getConnection(URL, USERNAME, PASSWORD)
          //          val conn = DriverManager.getConnection("jdbc:mysql://cdh1:3306/hive", "root", "yisa_omnieye")
          pool.push(conn)
          conNum += 1
          //          println("conNum" + conNum)
        }
      }
      var connection = pool.poll()
      if (connection.isClosed()) {
        conNum -= 1
        return getConnet(zkHostport: String)
      } else {
        return connection
      }
    })
  }

  //释放连接
  def releaseConn(conn: Connection): Unit = {
    pool.push(conn)
  }
  //加载驱动
  private def preGetConn(): Unit = {
    //控制加载
//    println("conNum" + conNum)
    if (conNum > max && pool.isEmpty) {
      println("Jdbc Pool has no connection now, please wait a moments!")
      Thread.sleep(2000)
      preGetConn()
    } else {
      Class.forName("com.mysql.jdbc.Driver");
    }
  }
}