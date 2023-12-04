package com.yisa.engine.db

import java.sql.Connection;
import scala.collection.mutable.Set
import scala.collection.mutable.Map
import com.yisa.wifi.zookeeper.ZookeeperUtil
import java.sql.DriverManager
import scala.collection.mutable.ArrayBuffer

/**
 * @author liliwei
 * @date  2016年9月9日
 * Mysql 单例连接
 */
object MySQLConnectManager extends Serializable {

  def getConnet(zkHostport: String): Connection = {
    //    var conn = JDBCConnectionSinglen.getConnet(zkHostport)
    var conn = C3p0ConnectionPool.getConnet(zkHostport)
    return conn
  }

  def initConnectionPool(zkHostPort: String) {
    C3p0ConnectionPool.initConnectionPool(zkHostPort)
    firstUseMysqlPool(zkHostPort)
  }

  private def firstUseMysqlPool(zkHostPort: String): Unit = {
    val conn = MySQLConnectManager.getConnet(zkHostPort)
    val sql = "select *  from  gpu_index_job limit 1";
    val pstmt = conn.prepareStatement(sql)
    pstmt.executeQuery();
    pstmt.close()
    conn.close()

  }

  def getAllYearidByBrandid(zkHostPort: String): Map[Int, Set[Long]] = {

    val DRIVER = "com.mysql.jdbc.Driver";
    val zkUtil = new ZookeeperUtil()
    val configs = zkUtil.getAllConfig(zkHostPort, "spark_engine", false)

    var jdbc = configs.get("spark_engine_gb_jdbc")
    val URL = jdbc
    val USERNAME = configs.get("spark_engine_gb_jdbc_username")
    val PASSWORD = configs.get("spark_engine_gb_jdbc_password")

    Class.forName(DRIVER);
    val conn = DriverManager.getConnection(URL, USERNAME, PASSWORD)

    var YearidByBrandid: Map[Int, Set[Long]] = Map[Int, Set[Long]]()

    val sql = "select yearID,brandID  from  gb.car_year";

    val pstmt = conn.prepareStatement(sql)
    val rs = pstmt.executeQuery();

    while (rs.next()) {

      val yearID = rs.getLong("yearID")
      val brandID = rs.getInt("brandID");

      if (YearidByBrandid.contains(brandID)) {

        var yearids = YearidByBrandid.get(brandID)
        yearids.get.add(yearID)
        YearidByBrandid.put(brandID, yearids.get)

      } else {
        var yearids: Set[Long] = Set()
        yearids.add(yearID)
        YearidByBrandid.put(brandID, yearids)
      }
    }
    pstmt.close()
    conn.close()

    YearidByBrandid

  }

  def getAllYearidByModelID(zkHostPort: String): Map[Int, Set[Long]] = {
    val DRIVER = "com.mysql.jdbc.Driver";
    val zkUtil = new ZookeeperUtil()
    val configs = zkUtil.getAllConfig(zkHostPort, "spark_engine", false)

    var jdbc = configs.get("spark_engine_gb_jdbc")
    val URL = jdbc
    val USERNAME = configs.get("spark_engine_gb_jdbc_username")
    val PASSWORD = configs.get("spark_engine_gb_jdbc_password")

    Class.forName(DRIVER);
    val conn = DriverManager.getConnection(URL, USERNAME, PASSWORD)
    
    var YearidByModelid: Map[Int, Set[Long]] = Map[Int, Set[Long]]()

    val sql = "select yearID,modelID  from  gb.car_year";

    val pstmt = conn.prepareStatement(sql)
    val rs = pstmt.executeQuery();

    while (rs.next()) {

      val yearID = rs.getLong("yearID");
      val modelID = rs.getInt("modelID");

      if (YearidByModelid.contains(modelID)) {
        var yearids = YearidByModelid.get(modelID)
        yearids.get.add(yearID)
        YearidByModelid.put(modelID, yearids.get)
      } else {
        var yearids: Set[Long] = Set()
        yearids.add(yearID)
        YearidByModelid.put(modelID, yearids)
      }
    }
    pstmt.close()
    conn.close()

    YearidByModelid

  }
  
  
   def getAllLocationid(zkHostPort: String): ArrayBuffer[String] = {
    val DRIVER = "com.mysql.jdbc.Driver";
    val zkUtil = new ZookeeperUtil()
    val configs = zkUtil.getAllConfig(zkHostPort, "spark_engine", false)

    var jdbc = configs.get("spark_engine_gb_jdbc")
    val URL = jdbc
    val USERNAME = configs.get("spark_engine_gb_jdbc_username")
    val PASSWORD = configs.get("spark_engine_gb_jdbc_password")

    Class.forName(DRIVER);
    val conn = DriverManager.getConnection(URL, USERNAME, PASSWORD)
    
    var allLocationid =ArrayBuffer[String]()

    val sql = "select location_id  from  gb.mon_location";

    val pstmt = conn.prepareStatement(sql)
    val rs = pstmt.executeQuery();

    while (rs.next()) {

      val location_id = rs.getString("location_id");
      allLocationid +=  location_id

    }
    pstmt.close()
    conn.close()

    allLocationid

  }

} 