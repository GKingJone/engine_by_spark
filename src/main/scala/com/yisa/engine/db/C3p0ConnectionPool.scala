package com.yisa.engine.db

import com.mchange.v2.c3p0.ComboPooledDataSource
import java.sql.Connection
import com.yisa.wifi.zookeeper.ZookeeperUtil

private[db] object C3p0ConnectionPool extends Serializable {
  @volatile private var ds: ComboPooledDataSource = null;

  def initConnectionPool(zkHostport: String) {

    if (ds == null) {
      println("initConnectionPool ---ing")
      synchronized {
        if (ds == null) {
          ds = new ComboPooledDataSource();
          val zkUtil = new ZookeeperUtil()
          val configs = zkUtil.getAllConfig(zkHostport, "spark_engine", false)
          val URL = configs.get("spark_engine_jdbc")
          val USERNAME = configs.get("spark_engine_jdbc_username")
          val PASSWORD = configs.get("spark_engine_jdbc_password")
          ds.setUser(USERNAME)
          ds.setPassword(PASSWORD)
          ds.setJdbcUrl(URL)
        }
      }
      println("initConnectionPool ---completed")
    }
  }


  def getConnet(zkHostport: String): Connection = {
    synchronized {
      initConnectionPool(zkHostport) 
      return ds.getConnection();
    }
    return null
  }
}