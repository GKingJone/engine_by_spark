package com.yisa.engine.branchV3

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.kafka.clients._
import org.apache.spark.sql.SparkSession
import com.yisa.engine.db.MySQLConnectManager
import com.google.gson.Gson
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.yisa.engine.uitl.TimeUtil
import java.text.SimpleDateFormat
import com.yisa.engine.common.InputBean
import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator
import com.facebook.presto.jdbc.PrestoConnection
import java.sql.DriverManager
import com.yisa.engine.db.MySQLConnectManager

/**
 * 频繁过车逻辑
 */
class SparkEngineV3ForFrequentlyCarPresto(line: String, tableName: String, resultTable: String, zkHostport: String, PrestoHostPort: String) extends Runnable {

  override def run() {
    val jdbcTable = resultTable

    var line_arr = line.split("\\|")

    val jobId = line_arr(1)
    val params = line_arr(2)

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val map: InputBean = gson.fromJson[InputBean](params, mapType)

    val sqlStr = gsonArrayFunc(map, params, tableName)

    println(sqlStr)

    //    val resultData = sparkSession.sql(sqlStr)

    println("SQL--------------:" + sqlStr)

    val count2 = map.count

    var PrestoURL = "jdbc:presto://" + PrestoHostPort + "/hive/yisadata"
    var PrestoUser = "spark"
    var PrestoPwd = ""
    var PrestoDriver = "com.facebook.presto.jdbc.PrestoDriver"

    Class.forName(PrestoDriver)
    val conn = DriverManager.getConnection(PrestoURL, PrestoUser, null)

    val stmt = conn.createStatement()

    val rs = stmt.executeQuery(sqlStr);

    var mysqlConn = MySQLConnectManager.getConnet(zkHostport)
    mysqlConn.setAutoCommit(false)
    var sql = "insert into " + jdbcTable + " (s_id,count,j_id,l_id) values(?,?,?,?)";
    var mysqlPstmt = mysqlConn.prepareStatement(sql);
    var count = 0

    while (rs.next()) {
      //      System.out.println(rs.getString(1));
      mysqlPstmt.setString(1, rs.getString(1));
      mysqlPstmt.setInt(2, rs.getInt(2));
      mysqlPstmt.setString(3, jobId);
      val l_id = rs.getString(3)
      if (l_id != null) {
        mysqlPstmt.setString(4, l_id);
      } else {
        mysqlPstmt.setString(4, "");
      }
      mysqlPstmt.addBatch();
      count = count + 1

    }
    mysqlPstmt.executeBatch()

    var sql2 = "insert into pfgc_count (j_id,count) values(?,?)";
    var mysqlpstmt2 = mysqlConn.prepareStatement(sql2);

    if (count == 0) {
      count = -1
    }
    mysqlpstmt2.setString(1, jobId);
    mysqlpstmt2.setInt(2, count);
    mysqlpstmt2.executeUpdate()
    mysqlpstmt2.close()

    mysqlConn.commit()
    mysqlPstmt.close()
    mysqlConn.close()

    rs.close();
    stmt.close()
    conn.close();

  }

  def gsonArrayFunc(map: InputBean, js: String, tableName: String): String = {

    //    val jobId: String = map.jobId
    //    val startTime = TimeUtil.getTimeStringFormat(map.startTime)
    //    val endTime = TimeUtil.getTimeStringFormat(map.endTime)
    val startTime = TimeUtil.getTimestampLong(map.startTime)
    val endTime = TimeUtil.getTimestampLong(map.endTime)

    val startTimeDateid = TimeUtil.getDateId(map.startTime)
    val endTimeDateid = TimeUtil.getDateId(map.endTime)

    val locationId: Array[String] = map.locationId
    val carModel: Array[String] = map.carModel
    val carBrand: String = map.carBrand
    val carYear: Array[String] = map.carYear
    val carColor: String = map.carColor
    val plateNumber: String = map.plateNumber
    val count: Int = map.count

    val sb = new StringBuffer()

    sb.append(" SELECT * FROM (")

    //    sb.append("SELECT first(solrid) as s_id, count(1) as count,plateNumber  as j_id FROM pass_info ")
    sb.append("SELECT arbitrary(solrid) as s_id, count(1) as count ,locationid as locationid  FROM  ").append(tableName)

    sb.append("  where  locationid !='' AND platenumber not like  '%无%' AND  platenumber not like  '%未%'  AND  platenumber not like  '%00000%'   ")
    sb.append("  AND length(platenumber) < 10     ")
    sb.append("  AND length(platenumber) > 6     ")

    if (startTime != 0L) {
      sb.append(" AND   capturetime >= ").append(startTime)
    }

    if (endTime != 0L) {
      sb.append(" AND capturetime <= ").append(endTime)
    }

    if (locationId != null && locationId.length > 0) {
      var l = locationId.map { "'" + _ + "'" }.reduce((a, b) => a + "," + b)
      sb.append(" AND locationid IN (").append(l).append(")")
    }

    if (carYear != null && carYear.length > 0) {
      var m = carYear.reduce((a, b) => a + "," + b)
      sb.append(" AND yearid IN (").append(m).append(")");
    }

    if (carModel != null && carModel.length > 0) {
      var m = carModel.reduce((a, b) => a + "," + b)
      sb.append(" AND modelid IN (").append(m).append(")");
    }

    if (carBrand != null && carBrand != "") {
      sb.append(" AND brandid = ").append(carBrand)
    }

    if (carColor != null && carColor != "") {
      sb.append(" AND colorId = ").append(carColor)
    }

    if (plateNumber != null && plateNumber != "") {
      if(plateNumber.contains("*") || plateNumber.contains("?")){
        sb.append(" AND plateNumber LIKE '%").append(plateNumber.replace("*", "%").replace("?", "_")).append("%'")
      } else {
        sb.append(" AND plateNumber = '").append(plateNumber).append("'")
      }
    }

    /**
     *
     *  val startTimeDateid = TimeUtil.getDateId(map.startTime)
     * val endTimeDateid = TimeUtil.getDateId(map.endTime)
     *
     */

    if (startTime != 0L) {
      sb.append(" AND dateid >= ").append(startTimeDateid)
    }

    if (endTime != 0L) {
      sb.append(" AND dateid <= ").append(endTimeDateid)
    }

    sb.append(" GROUP BY plateNumber, locationid  ")
    //order by count limit 10000
    sb.append(" ) cenTable WHERE count >= ").append(count)
    sb.append(" order by count desc ")
    sb.append("  limit 1000")

    sb.toString()
  }

}

