package com.yisa.engine.branchV5

import java.sql.ResultSet
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.yisa.engine.common.InputBean
import com.yisa.engine.db.MySQLConnectManager
import com.yisa.engine.uitl.TimeUtil

/**
 * 昼伏夜出逻辑
 */
class SparkEngineV5ForNocturnal extends Serializable {

  var map: InputBean = null

  def Nocturnal(sparkSession: SparkSession, line: String, tableName: String, resultTable: String, zkHostport: String): Unit = {

    val jdbcTable = "nocturnal_result"
    //分割
    var line_arr = line.split("\\|")
    //分离jobId和参数
    val jobId = line_arr(1)
    val params = line_arr(2)

    val sqlStr = gsonArrayFunc(params, tableName)

    println("SQL--------------:" + sqlStr)

    
    val resultData = sparkSession.sql(sqlStr)

    val longAccumulator  = new LongAccumulator ()
    sparkSession.sparkContext.register(longAccumulator, "longAccumulator")
    longAccumulator.reset()
    
    resultData.foreachPartition { data =>
      var conn = MySQLConnectManager.getConnet(zkHostport)
      conn.setAutoCommit(false)

      var sql = "insert into " + jdbcTable + " (s_id,d_time,j_id,l_c,is_sheltered) values(?,?,?,?,?)";
      var pstmt = conn.prepareStatement(sql);

      data.foreach { t =>
        {

          pstmt.setString(1, t(0).toString());
          pstmt.setInt(2, t(1).toString().toInt);
          pstmt.setString(3, jobId);
          pstmt.setInt(4, t(2).toString().toInt);
          pstmt.setInt(5, t(3).toString().toInt);
          pstmt.addBatch();
          
          longAccumulator.add(1L)

        }
      }

      pstmt.executeBatch()
      conn.commit()
      pstmt.close()
      conn.close()

    }

    var conn = MySQLConnectManager.getConnet(zkHostport)
    conn.setAutoCommit(false)

//    val sql3 = "select count(*) from nocaturnal_result where j_id=?";
//    var pstmt3 = conn.prepareStatement(sql3);
//    pstmt3.setString(1, jobId);
//    val set = pstmt3.executeQuery();
//    val total = set.getInt(0)
//    pstmt3.close();
    
    var  total =  longAccumulator.value

    var sql2 = "insert into nocturnal_count (j_id,total) values(?,?)";
    var pstmt2 = conn.prepareStatement(sql2);

    if (total == 0) {
      total = -1L
    }
    pstmt2.setString(1, jobId);
    pstmt2.setInt(2, total.toInt);
    pstmt2.executeUpdate()
    conn.commit()
    pstmt2.close()
    conn.close()

  }

  def gsonArrayFunc(js: String, tableName: String): String = {

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    map = gson.fromJson[InputBean](js, mapType)

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
    
    sb.append("select max_by(solrid,capturetime) as s_id,COUNT(DISTINCT dateid) as d_time,first(lastcaptured) as l_c,first(issheltered) as is_sheltered FROM  ").append(tableName)

    sb.append("  WHERE  locationid !='' AND platenumber not like  '%无%' AND  platenumber not like  '%未%'   AND  platenumber not like  '%00000%'  ")

    val sdf = new SimpleDateFormat("yyyyMMdd")

    val interval = TimeUtil.getIntervalByDay(sdf.parse(startTime), sdf.parse(endTime))

    sb.append(" AND (  ")
    if (nightTime1.toLong < nightTime2.toLong) { //夜出第一个时间小于第二个时间
      sb.append("( (capturetime between ").append(TimeUtil.getTimestampLong(startTime + nightTime1)).append(" AND ").append(TimeUtil.getTimestampLong(startTime + nightTime2)).append(" ) AND (capturetime not between ").append(TimeUtil.getTimestampLong(startTime + dayTime1)).append(" AND ").append(TimeUtil.getTimestampLong(startTime + dayTime2)).append(" ) ) ")
      for (num <- 1 to interval) {
        val time = TimeUtil.beforNumberDay(sdf.parse(startTime), interval)
        sb.append(" OR ( (capturetime between ").append(TimeUtil.getTimestampLong(time + nightTime1)).append(" AND ").append(TimeUtil.getTimestampLong(time + nightTime2)).append(") AND (capturetime not between ").append(TimeUtil.getTimestampLong(startTime + dayTime1)).append((time + dayTime1).toLong).append(" AND ").append(TimeUtil.getTimestampLong(time + dayTime2)).append(") ) ")
      }

    }

    if (nightTime1.toLong > nightTime2.toLong) { //夜出第一个时间大于第二个时间
      dateids = endTime+1
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
      var l = locationId.map { "'" + _ + "'" }.reduce((a, b) => a + "," + b)
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


