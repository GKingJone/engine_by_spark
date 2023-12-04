package com.yisa.engine.branchV5

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.kafka.clients._
import org.apache.spark.sql.SparkSession
import com.yisa.engine.db.MySQLConnectManager
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.yisa.engine.uitl.TimeUtil
import java.text.SimpleDateFormat
import com.yisa.engine.common.InputBean
import org.apache.spark.SparkContext
import com.yisa.engine.db.HBaseConnectManager
import java.sql.Timestamp
import java.util.Date
import scala.collection.mutable.HashMap
import java.util.TreeMap

/**
 * 超时分析
 *
 */
class SparkEngineV5ForPassTimeout extends Serializable {

  def PassTimeout(sparkSession: SparkSession, line: String, tableName: String, zkHostport: String): Unit = {

    val jdbcTable = "togetherCar_result"

    var line_arr = line.split("\\|")

    val jobId = line_arr(1)
    val params = line_arr(2)

    println(params)

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val inputBean: InputBean = gson.fromJson[InputBean](params, mapType)

    val diff = inputBean.differ * 60
    val locationId: Array[String] = inputBean.locationId

    val sqlStr = getSQL(inputBean, tableName)

    val allPass: HashMap[String, TreeMap[Long, String]] = new HashMap[String, TreeMap[Long, String]]

    val resultData = sparkSession.sql(sqlStr)

    println("SQL--------------:" + sqlStr)

    val resultData2 = resultData.collect()

    resultData2.foreach(result => {

      //locationid ,capturetime,platenumber,solrid
      val locationid = result.getString(0)
      val capturetime = result.getLong(1)
      val platenumber = result.getString(2)
      val solrid = result.getString(3)
      if (allPass.contains(platenumber)) {
        val treeMap = allPass(platenumber)
        treeMap.put(capturetime, locationid + "_" + platenumber + "_" + solrid)
        allPass.+=(platenumber -> treeMap)
      } else {
        val treeMap = new TreeMap[Long, String]
        treeMap.put(capturetime, locationid + "_" + platenumber + "_" + solrid)
        allPass.+=(platenumber -> treeMap)
      }
    })

    var firstGot = false

    var pass_timeout_count = 0
    var conn = MySQLConnectManager.getConnet(zkHostport)
    conn.setAutoCommit(false)
    var sql = "insert into pass_timeout_result (job_id,platenumber,in_id,out_id) values(?,?,?,?)";
    var pstmt = conn.prepareStatement(sql);

    allPass.foreach(onePass => {
      val treeMap = onePass._2
      if (treeMap.size() >= 2) {

        val treeMap_i = treeMap.entrySet().iterator()

        var pass_start_time = 0L
        var pass_end_time = 0L

        var pre_start_solrid = ""
        var pre_end_solrid = ""

        while (treeMap_i.hasNext()) {
          val treeMap_one = treeMap_i.next()
          if(onePass._1.equals("鄂AZX360")){
        	  println("treeMap_one.getValue:" + treeMap_one.getValue)
          }

          if (pass_start_time > 0
            && pass_end_time > 0
            && !pre_start_solrid.equals("")
            && !pre_end_solrid.equals("")) {

            val diff_2 = pass_end_time - pass_start_time

            if (diff_2 >= diff) {
              pstmt.setString(1, jobId);
              pstmt.setString(2, onePass._1);
              pstmt.setString(3, pre_start_solrid);
              pstmt.setString(4, pre_end_solrid);
              pstmt.addBatch();
              pass_timeout_count = pass_timeout_count + 1
            }
            pass_start_time = 0L
            pass_end_time = 0L
            pre_start_solrid = ""
            pre_end_solrid = ""
          }

          if(!firstGot){
        	  firstGot = treeMap_one.getValue.split("_")(0).equals(locationId(0))
          }
          if (firstGot && treeMap_one.getValue.split("_")(0).equals(locationId(0))) {
            pass_start_time = treeMap_one.getKey
            pre_start_solrid = treeMap_one.getValue.split("_")(2)
          }
          if (firstGot && treeMap_one.getValue.split("_")(0).equals(locationId(1))) {
            pass_end_time = treeMap_one.getKey
            pre_end_solrid = treeMap_one.getValue.split("_")(2)
          }

        }

      }

    })

    var sql2 = "insert into pass_timeout_count (job_id,total) values(?,?)";
    var pstmt2 = conn.prepareStatement(sql2);

    if (pass_timeout_count == 0) {
      pass_timeout_count = -1
    }

    println("pass_timeout_count:" + pass_timeout_count)
    pstmt2.setString(1, jobId);
    pstmt2.setInt(2, pass_timeout_count);

    pstmt.executeBatch()
    pstmt.close()

    pstmt2.executeUpdate()
    pstmt2.close()

    conn.commit()
    conn.close()

  }

  def getSQL(travelTogethers: InputBean, tableName: String): String = {

    val startTime = TimeUtil.getTimestampLong(travelTogethers.startTime)
    val endTime = TimeUtil.getTimestampLong(travelTogethers.endTime)

    val startTimeDateid = TimeUtil.getDateId(travelTogethers.startTime)
    val endTimeDateid = TimeUtil.getDateId(travelTogethers.endTime)

    val sb = new StringBuffer()
    sb.append("  SELECT locationid ,capturetime,platenumber,solrid ")

    sb.append(" FROM ").append(tableName)

    //    sb.append(" JOIN  ").append(" ON p.locationid = t.locationid ")
    sb.append(" WHERE  ")

    if (startTime != 0L) {
      sb.append("  dateid >= ").append(startTimeDateid)
    }

    if (endTime != 0L) {
      sb.append(" AND dateid <= ").append(endTimeDateid)
    }

    if (travelTogethers.carYear != null && travelTogethers.carYear.size > 0 && travelTogethers.carYear(0) != "" && travelTogethers.carYear(0) != "0") {
      var m = travelTogethers.carYear.reduce((a, b) => a + "," + b)
      sb.append(" AND yearid IN (").append(m).append(")");
    }

    if (startTime != 0L) {
      sb.append(" AND capturetime >= ").append(startTime)
    }

    if (endTime != 0L) {
      sb.append(" AND capturetime <= ").append(endTime)
    }

    val locationId: Array[String] = travelTogethers.locationId

    val carBrand = travelTogethers.carBrand

    val carModel = travelTogethers.carModel

    val carLevel = travelTogethers.carLevel

    val carColor = travelTogethers.carColor

    if (locationId != null && locationId.length > 0) {
      var l = locationId.map { "'" + _ + "'" }.reduce((a, b) => a + "," + b)
      sb.append(" AND locationid IN (").append(l).append(")")
    }

    if (carModel != null && carModel.length > 0 && travelTogethers.carModel(0) != "0") {
      var m = carModel.reduce((a, b) => a + "," + b)
      sb.append(" AND modelid IN (").append(m).append(")");
    }

    if (carBrand != null && carBrand != "" && carBrand != "0") {
      sb.append(" AND brandid = ").append(carBrand)
    }
    if (carLevel != null && carLevel.length > 0 && travelTogethers.carLevel(0) != "0") {
      var m = carLevel.reduce((a, b) => a + "," + b)
      sb.append(" AND levelid IN (").append(m).append(")");
    }

    if (carColor != null && carColor != "" && carColor != "0") {
      sb.append(" AND colorid = ").append(carColor)
    }
    sb.toString()
  }

}


