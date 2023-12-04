package com.yisa.engine.branchV3

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import com.yisa.engine.db.MySQLConnectManager
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.yisa.engine.uitl.TimeUtil
import com.yisa.engine.common.InputBean
import java.util.Date

class SparkEngineV3ForFrequentlyCar(sparkSession: SparkSession, line: String, tableName: String, resultTable: String, zkHostport: String) extends Runnable {

  override def run() {
 
    val date1 = new Date().getTime
    //    val jdbcTable = "pfgc_result_spark"
    val jdbcTable = resultTable

    var line_arr = line.split("\\|")

    val jobId = line_arr(1)
    val params = line_arr(2)

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val map: InputBean = gson.fromJson[InputBean](params, mapType)

    val sqlStr = getSQL(map, tableName)

    val resultData = sparkSession.sql(sqlStr)

    println("SQL--------------:" + sqlStr)
 
    val count2 = map.count

    val resultData2 = resultData.collect()

    var conn = MySQLConnectManager.getConnet(zkHostport)
    conn.setAutoCommit(false)
    var sql = "insert into " + jdbcTable + " (s_id,count,j_id,l_id) values(?,?,?,?)";
    var pstmt = conn.prepareStatement(sql);
    var count = 0

    resultData.foreachPartition { data =>

      var conn = MySQLConnectManager.getConnet(zkHostport)
      conn.setAutoCommit(false)
      var sql = "insert into " + jdbcTable + " (s_id,count,j_id,l_id) values(?,?,?,?)";
      var pstmt = conn.prepareStatement(sql);
      var count = 0
      data.foreach { t =>
        {

          //          if (t(1).toString().toInt >= count2) {

          pstmt.setString(1, t(0).toString());
          pstmt.setInt(2, t(1).toString().toInt);
          pstmt.setString(3, jobId);
          if (t(2) != null) {
            pstmt.setString(4, t(2).toString());
          } else {
            pstmt.setString(4, "");
          }
          pstmt.addBatch();
          count += 1

        }
      }

      var sql2 = "insert into pfgc_count (j_id,count) values(?,?)";
      var pstmt2 = conn.prepareStatement(sql2);

      if (count == 0) {
        count = -1
      }
      pstmt2.setString(1, jobId);
      pstmt2.setInt(2, count);
      pstmt2.executeUpdate()
      pstmt2.close()

      pstmt.executeBatch()
      conn.commit()
      pstmt.close()
      conn.close()
    }
    val date2 = new Date().getTime
    println("SparkEngineV3ForFrequentlyCar  time :" + (date2 - date1))
  }
  def getSQL(map: InputBean, tableName: String): String = {

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
    sb.append("SELECT first(solrid) as s_id, count(1) as count ,locationid as locationid  FROM  ").append(tableName)

    sb.append("  where  locationid !='' AND platenumber not like  '%无%'  AND  platenumber not like  '%00000%'  ")

    if (startTime != 0L) {
      sb.append(" AND   capturetime >= ").append(startTime)
    }

    if (endTime != 0L) {
      sb.append(" AND capturetime <= ").append(endTime)
    }

    //    if (startTime != null) {
    //      sb.append(" WHERE capturetime >= '").append(startTime).append("'")
    //    }
    //
    //    if (endTime != null) {
    //      sb.append(" AND capturetime <= '").append(endTime).append("'")
    //    }

    if (locationId != null && locationId.length > 0) {
      var l = locationId.map { "'" + _ + "'" }.reduce((a, b) => a + "," + b)
      //   		  var l = locationId.map { "" + _ + "" }.reduce((a, b) => a + "," + b)
      sb.append(" AND locationid IN (").append(l).append(")")
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
        sb.append(" AND plateNumber LIKE '").append(plateNumber).append("'")
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
    sb.append(" ) cenTable WHERE count > ").append(count)
    sb.append(" order by count desc ")
    sb.append("  limit 1000")

    sb.toString()
  }
}