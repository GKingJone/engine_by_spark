package com.yisa.engine.branchV5

import java.sql.DriverManager
import com.yisa.engine.db.MySQLConnectManager
import com.yisa.engine.common.InputBean
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import java.text.SimpleDateFormat
import com.yisa.engine.uitl.TimeUtil

/**
  * 昼伏夜出逻辑
  */
class SparkEngineV5ForNocturnalPresto(line: String, tableName: String, resultTable: String, zkHostPort: String, prestoHostPort: String) extends Runnable {

  override def run() {
    val jdbcTable = "nocturnal_result"
    var line_arr = line.split("\\|")

    val jobId = line_arr(1)
    val params = line_arr(2)

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val map: InputBean = gson.fromJson[InputBean](params, mapType)

    val sqlStr = gsonArrayFunc(map, params, tableName)

    println(sqlStr)


    var PrestoURL = "jdbc:presto://" + prestoHostPort + "/hive/yisadata"
    val PrestoUser = "spark"
    //var PrestoPwd = ""
    var PrestoDriver = "com.facebook.presto.jdbc.PrestoDriver"

    Class.forName(PrestoDriver)
    val conn = DriverManager.getConnection(PrestoURL, PrestoUser, null)

    val stmt = conn.createStatement()

    val rs = stmt.executeQuery(sqlStr);

    var mysqlConn = MySQLConnectManager.getConnet(zkHostPort)
    mysqlConn.setAutoCommit(false)
    var sql = "insert into " + jdbcTable + " (s_id,d_time,j_id,l_c,is_sheltered) values(?,?,?,?,?)";
    var mysqlPstmt = mysqlConn.prepareStatement(sql);
    var count = 0
    while (rs.next()) {
      mysqlPstmt.setString(1, rs.getString(1));
      mysqlPstmt.setInt(2, rs.getInt(2));
      mysqlPstmt.setString(3, jobId);
      mysqlPstmt.setInt(4, rs.getInt(3));
      mysqlPstmt.setInt(5, rs.getInt(4));
      mysqlPstmt.addBatch();
      count = count + 1

    }
    mysqlPstmt.executeBatch()

    var sql2 = "insert into nocturnal_count (j_id,total) values(?,?)";
    var mysqlpstmt2 = mysqlConn.prepareStatement(sql2);

    if (count == 0) {
      count = -1
    }
    mysqlpstmt2.setString(1, jobId);
    mysqlpstmt2.setInt(2, count);
    mysqlpstmt2.executeUpdate()

    mysqlConn.commit()
    mysqlpstmt2.close()
    mysqlPstmt.close()
    mysqlConn.close()

    rs.close();
    stmt.close();
    conn.close();
  }

  def gsonArrayFunc(map: InputBean, js: String, tableName: String): String = {

    var dateStr = map.carColor.split(",")
    val startTime = dateStr(0)
    val endTime = dateStr(1)

    var dayStr = map.startTime.split(",")
    val dayTime1 = dayStr(0)
    val dayTime2 = dayStr(1)

    var nightStr = map.endTime.split(",")
    val nightTime1 = nightStr(0)
    val nightTime2 = nightStr(1)

    val locationId: Array[String] = map.locationId
    val carModel: Array[String] = map.carModel
    val carBrand: String = map.carBrand
    val carYear: Array[String] = map.carYear
    val carLevel: Array[String] = map.carLevel
    val noPlateNumber = map.plateNumber

    val sb = new StringBuffer()

    var dateids = endTime

    sb.append("select max_by(solrid,capturetime) as s_id,COUNT(DISTINCT dateid) as d_time,arbitrary(lastcaptured) as l_c,arbitrary(issheltered) as is_sheltered FROM  ").append(tableName)

    sb.append("  WHERE  locationid !='' AND platenumber not like  '%无%' AND  platenumber not like  '%未%'   AND  platenumber not like  '%00000%'  ")

    val sdf = new SimpleDateFormat("yyyyMMdd")

    val interval = TimeUtil.getIntervalByDay(sdf.parse(startTime), sdf.parse(endTime))

    sb.append(" AND (  ")
    if (nightTime1.toLong < nightTime2.toLong) { //夜出第一个时间小于第二个时间
      sb.append("( (capturetime between ").append(TimeUtil.getTimestampLong(startTime + nightTime1)).append(" AND ").append(TimeUtil.getTimestampLong(startTime + nightTime2)).append(" ) AND (capturetime not between ").append(TimeUtil.getTimestampLong(startTime + dayTime1)).append(" AND ").append(TimeUtil.getTimestampLong(startTime + dayTime2)).append(" ) ) ")
      for (num <- 1 to interval) {
        val time = TimeUtil.beforNumberDay(sdf.parse(startTime), interval)
        sb.append(" OR ( (capturetime between ").append(TimeUtil.getTimestampLong(time + nightTime1)).append(" AND ").append(TimeUtil.getTimestampLong(time + nightTime2)).append(") AND (capturetime not between ").append(TimeUtil.getTimestampLong(startTime + dayTime1)).append(" AND ").append(TimeUtil.getTimestampLong(time + dayTime2)).append(") ) ")
      }

    }

    if (nightTime1.toLong > nightTime2.toLong) { //夜出第一个时间大于第二个时间
      dateids = endTime + 1
      val startDateNight = TimeUtil.beforNumberDay(sdf.parse(startTime), 1)
      sb.append("( (capturetime between ").append(TimeUtil.getTimestampLong(startTime + nightTime1)).append(" AND ").append(TimeUtil.getTimestampLong(startDateNight + nightTime2)).append(") AND (capturetime not between ").append(TimeUtil.getTimestampLong(startTime + dayTime1)).append(" AND ").append(TimeUtil.getTimestampLong(startTime + dayTime2)).append(") ) ")
      for (num <- 1 to interval) {
        val time = TimeUtil.beforNumberDay(sdf.parse(startTime), interval)
        val time2 = TimeUtil.beforNumberDay(sdf.parse(startTime), interval + 1)
        sb.append(" OR ( (capturetime between ").append(TimeUtil.getTimestampLong(time + nightTime1)).append(" AND ").append(TimeUtil.getTimestampLong(time2 + nightTime2)).append(") AND (capturetime not between ").append(TimeUtil.getTimestampLong(startTime + dayTime1)).append(" AND ").append(TimeUtil.getTimestampLong(time + dayTime2)).append(") ) ")
      }
    }
    sb.append(")")
    if (noPlateNumber != null && noPlateNumber != "") {
      sb.append(" AND platenumber != ").append(noPlateNumber)
    }

    if (locationId != null && locationId.length > 0) {
      var l = locationId.map {
        "'" + _ + "'"
      }.reduce((a, b) => a + "," + b)
      //   		  var l = locationId.map { "" + _ + "" }.reduce((a, b) => a + "," + b)
      sb.append(" AND locationid IN (").append(l).append(")")
    }

    if (carModel != null && carModel.length > 0) {
      var m = carModel.reduce((a, b) => a + "," + b)
      sb.append(" AND modelid IN (").append(m).append(")");
    }

    if (carYear != null && carYear.length > 0) {
      var m = carYear.reduce((a, b) => a + "," + b)
      sb.append(" AND yearid IN (").append(m).append(")");
    }

    if (carBrand != null && carBrand != "") {
      sb.append(" AND brandid = ").append(carBrand)
    }

    if (carLevel != null && carLevel.length > 0) {
      var m = carLevel.reduce((a, b) => a + "," + b)
      sb.append(" AND levelid IN (").append(m).append(")");
    }

    sb.append(" AND dateid >=").append(startTime.toInt).append(" AND dateid<=").append(dateids.toInt)

    sb.append(" group by platenumber")

    sb.toString()
  }
}