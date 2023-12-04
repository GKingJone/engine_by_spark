package com.yisa.engine.branchV3

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.util.Base64
import org.apache.spark.sql.SparkSession
import com.yisa.engine.db.MySQLConnectManager
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.yisa.engine.common.InputBean
import com.yisa.engine.uitl.TimeUtil
import java.util.Date
import java.sql.DriverManager
import java.sql.ResultSet

/**
 * @author liliwei
 * @date  2016年9月9日
 * 以图搜车
 */
class SparkEngineV3ForSearchCarByPicPresto(sqlContext: SparkSession, line: String, tableName: String, zkHostport: String, PrestoHostPort: String) extends Runnable {

  override def run() {

    val now1 = new Date().getTime()
    var line_arr = line.split("\\|")
    val job_id = line_arr(1)

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val map: InputBean = gson.fromJson[InputBean](line_arr(2), mapType)

    //得到执行SQL
    val sql1 = getSQL(map, tableName, true)

    println("SQL--------------:" + sql1)
    
    //get Presto JDBC

    var PrestoURL = "jdbc:presto://" + PrestoHostPort + "/hive/yisadata"
    var PrestoUser = "sparkPic"
    var PrestoPwd = ""
    var PrestoDriver = "com.facebook.presto.jdbc.PrestoDriver"

    Class.forName(PrestoDriver)
    val conn = DriverManager.getConnection(PrestoURL, PrestoUser, null)

    val stmt = conn.createStatement()

    val rs = stmt.executeQuery(sql1);

    val count = resultInRDBMS(rs, zkHostport, job_id)
    rs.close();

    //< 20000 hasnt data,run again without simily limit
    if (count == 0) {
      println("< 20000 hasnt data,run again without simily limit")
      //得到执行SQL
      val sql2 = getSQL(map, tableName, false)

      println("SQL--------------:" + sql2)
      val rs2 = stmt.executeQuery(sql2);
      val count = resultInRDBMS(rs2, zkHostport, job_id)
      rs2.close()
    }

    println("result conut --------------:" + count)

    stmt.close()
    conn.close();

  }

  def resultInRDBMS(rs: ResultSet, zkHostport: String, job_id: String): Int = {

    var mysqlConn = MySQLConnectManager.getConnet(zkHostport)
    mysqlConn.setAutoCommit(false)
    val sql = "insert into gpu_index_job (job_id,solr_id,sort_value,plate_number) values(?,?,?,?)";
    var mysqlPstmt = mysqlConn.prepareStatement(sql);
    var count = 0

    while (rs.next()) {
      //      System.out.println(rs.getString(1));
      mysqlPstmt.setString(1, job_id);
      mysqlPstmt.setString(2, rs.getString(1));
      mysqlPstmt.setInt(3, rs.getInt(2));
      mysqlPstmt.setString(4, rs.getString(3));
      val l_id = rs.getString(3)

      mysqlPstmt.addBatch();
      count = count + 1

    }
    mysqlPstmt.executeBatch()

    mysqlConn.commit()
    mysqlPstmt.close()
    mysqlConn.close()

    return count

  }

  def getSQL(map: InputBean, tableName: String, similarityLimit: Boolean) = {
    val feature = map.feature

    val carBrand = map.carBrand

    val carModel = map.carModel

    //    val startTime = TimeUtil.getTimeStringFormat(map.startTime)
    //    val endTime = TimeUtil.getTimeStringFormat(map.endTime)
    val startTime = TimeUtil.getTimestampLong(map.startTime)
    val endTime = TimeUtil.getTimestampLong(map.endTime)

    val sb = new StringBuffer();

    sb.append(" SELECT  solrid,similarity ,plateNumber FROM ")
    sb.append(" (")
    sb.append(" SELECT solrid,ImageSimilay('" + feature + "',recFeature) as similarity,plateNumber as plateNumber ")

    sb.append(" FROM " + tableName + " where  recFeature != '' and recFeature != 'null' ")

    if (map.carYear != null && map.carYear(0) != "" && map.carYear(0) != "0") {
      var m = map.carYear.reduce((a, b) => a + "," + b)
      sb.append(" AND yearid IN (").append(m).append(")");
    }

    if (startTime != 0L) {
      sb.append(" AND dateid >= ").append(TimeUtil.getDateId(map.startTime))
      sb.append(" AND capturetime >= ").append(startTime)
    }

    if (endTime != 0L) {
      sb.append(" AND dateid <= ").append(TimeUtil.getDateId(map.endTime))
      sb.append(" AND capturetime <= ").append(endTime)
    }

    if (carModel != null && carModel.length > 0 && map.carModel(0) != "0") {
      var m = carModel.reduce((a, b) => a + "," + b)
      sb.append(" AND modelid IN (").append(m).append(")");
    }

    if (carBrand != null && carBrand != "" && carBrand != "0") {
      sb.append(" AND brandid = ").append(carBrand)
    }

    //    sb.append("  and modelId  = " + modelId)
    sb.append(" ) t  ")

    if (similarityLimit) {
      sb.append(" where ")
      sb.append(" t.similarity< 20000")
    }

    sb.append(" order by similarity limit 100  ")
    sb.toString()
  }

}
 




  





