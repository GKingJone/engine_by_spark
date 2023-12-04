package com.yisa.engine.db

import java.sql.DriverManager
import java.sql.Connection;
import com.yisa.wifi.zookeeper.ZookeeperUtil

/**
 * @author liliwei
 * @date  2016年9月9日
 * Mysql 单例连接
 */
private[db] object JDBCConnectionSinglen {
  @volatile private var conn: Connection = null;

  //  val confNames = "file:///app/spark/spark_engine.properties";
  //val properties = new Properties()
  //
  //properties.load(new FileInputStream(confNames))

  //var zknode = properties.getProperty("zkHostport")
  //val USERNAME = "root"
  // val PASSWORD = "root"
  //    val URL = "jdbc:mysql://128.127.120.200:3306/yisaoe?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8&useServerPrepStmts=false&rewriteBatchedStatements=true";
  //  val URL = "jdbc:mysql://10.64.203.127:3306/yisaoe?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8&useServerPrepStmts=false&rewriteBatchedStatements=true";

  val DRIVER = "com.mysql.jdbc.Driver";

  def getConnet(zkHostport: String): Connection = {

    if (conn == null || conn.isClosed()) {
      synchronized {
        if (conn == null|| conn.isClosed()) {

          val zkUtil = new ZookeeperUtil()
          val configs = zkUtil.getAllConfig(zkHostport, "spark_engine", false)

          var jdbc = configs.get("spark_engine_jdbc")
          val URL = jdbc
          val USERNAME = configs.get("spark_engine_jdbc_username")
          val PASSWORD = configs.get("spark_engine_jdbc_password")

          Class.forName(DRIVER);
          conn = DriverManager.getConnection(URL, USERNAME, PASSWORD)
        }
      }
    }
    conn
  }

  def getConnet2: Connection = {

    if (conn == null|| conn.isClosed()) {
      synchronized {
        if (conn == null) {
          val URL = "jdbc:mysql://128.127.120.200:3306/yisaoe?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8&useServerPrepStmts=false&rewriteBatchedStatements=true";

          val USERNAME = "root"
          val PASSWORD = "root"

          Class.forName(DRIVER);
          conn = DriverManager.getConnection(URL, USERNAME, PASSWORD)
        }
      }
    }
    conn
  }

}