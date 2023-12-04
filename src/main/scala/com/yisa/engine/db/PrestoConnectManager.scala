package com.yisa.engine.db

import java.sql.Connection;

/**
 * @author liliwei
 * @date  2016年9月9日
 * Mysql 单例连接
 */
object PrestoConnectManager extends Serializable {

  def getConnet(zkHostport: String): Connection = {
    //    var conn = JDBCConnectionSinglen.getConnet(zkHostport)
    var conn = C3p0ConnectionPool.getConnet(zkHostport)
    return conn
  }
 
  def initConnectionPool(zkHostport: String) {
    C3p0ConnectionPool.initConnectionPool(zkHostport)
  }

} 