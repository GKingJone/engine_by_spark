package com.yisa.engine.branch

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
import com.yisa.engine.common.Locationid_detail
import com.yisa.engine.common.Locationid_detail
import com.yisa.engine.common.Locationid_detail
import java.util.ArrayList
import java.sql.DriverManager

/**
 * 同行车辆
 *
 *
 * 注意：
 * 1、如果传入请求内有车型等信息，可能会出现，不传车型时，某个车牌出现，传了车型之后，车牌反而出不来。是因为这个车牌的每一条过车记录不是都识别出了车
 * 车型等信息。当限制车型之后，此车牌的过车量变少了，此时，调低“次数”值 ，这个车牌会再次出现
 *
 * 2、程序内取的Yarid是最大值，也就是说当某个车牌被识别为多个yearid时，只会返回最大的那个
 *
 */
class SparkEngineV2ForTravelTogetherPresto {

  def TravelTogether(sparkSession: SparkSession, line: String, tableName: String, zkHostport: String, PrestoHostPort: String): Unit = {

    val jdbcTable = "togetherCar_result"

    var line_arr = line.split("\\|")

    val jobId = line_arr(1)
    val params = line_arr(2)

    println(params)

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val travelTogether: InputBean = gson.fromJson[InputBean](params, mapType)

    val countT = travelTogether.count

    val sqlStr = getSQL(travelTogether, params, tableName)
    println("SQL--------------:" + sqlStr)

    var PrestoURL = "jdbc:presto://" + PrestoHostPort + "/hive/yisadata"
    var PrestoUser = "spark"
    var PrestoPwd = ""
    var PrestoDriver = "com.facebook.presto.jdbc.PrestoDriver"

    Class.forName(PrestoDriver)
    val conn = DriverManager.getConnection(PrestoURL, PrestoUser, null)

    val stmt = conn.createStatement() 

    val rs = stmt.executeQuery(sqlStr);

  }

  def registTmpTable(travelTogethers: InputBean, tableName: String): String = {

    val startTimeDateid = TimeUtil.getDateId(travelTogethers.startTime)
    val endTimeDateid = TimeUtil.getDateId(travelTogethers.endTime)

    val sql = new StringBuffer();
    sql.append("SELECT locationid,capturetime ").append(" FROM ").append(tableName);
    sql.append(" WHERE platenumber = '").append(travelTogethers.plateNumber).append("'");

    sql.append(" AND capturetime   >= ").append(TimeUtil.getTimestampLong((travelTogethers.startTime)));
    sql.append(" AND capturetime   <= ").append(TimeUtil.getTimestampLong((travelTogethers.endTime)));

    if (travelTogethers.locationId != null && travelTogethers.locationId.length != 0 && travelTogethers.locationId(0) != "" && travelTogethers.locationId(0) != "0") {
      var l = travelTogethers.locationId.map { "'" + _ + "'" }.reduce((a, b) => a + "," + b)
      sql.append(" AND locationid IN (").append(l).append(")")
    }

    sql.append(" AND dateid >= ").append(startTimeDateid)

    sql.append(" AND dateid <= ").append(endTimeDateid)

    return sql.toString();

  }

  def getSQL(travelTogethers: InputBean, js: String, tableName: String): String = {

    val startTime = TimeUtil.getTimestampLong(travelTogethers.startTime)
    val endTime = TimeUtil.getTimestampLong(travelTogethers.endTime)

    val startTimeDateid = TimeUtil.getDateId(travelTogethers.startTime)
    val endTimeDateid = TimeUtil.getDateId(travelTogethers.endTime)

    val sb = new StringBuffer()
    sb.append("  SELECT solrids,platenumber,days, locationids,total,issheltered,lastcaptured,yearid ")
    sb.append("  FROM (")
    sb.append(" SELECT concat_ws(',',collect_set(o.solrid )) AS solrids ,o.platenumber,")
    sb.append(" size(collect_set(o.dateid )) AS days,collect_list(o.locationid) AS locationids, ")
    sb.append(" count(o.solrid) AS total,max(o.issheltered) AS issheltered,max(o.lastcaptured) AS lastcaptured,")
    sb.append(" first(o.yearid) AS yearid  ")

    sb.append(" FROM  ( ")

    sb.append(" SELECT distinct p.solrid,p.platenumber,p.dateid,p.locationid,p.solrid,p.issheltered,p.lastcaptured,p.yearid")

    sb.append(" FROM ").append(tableName).append(" p  ")

    sb.append(" JOIN  ");
    sb.append(" ( ");
    
    sb.append(registTmpTable(travelTogethers, tableName));

    sb.append(" ) ");

    sb.append(" t ").append(" ON p.locationid = t.locationid ")
    sb.append(" WHERE  ").append("  p.locationid = t.locationid ")

    sb.append(" AND   platenumber not like  '%无%' AND  platenumber not like  '%未%'   AND  platenumber not like  '%00000%'  ")

    sb.append(" AND platenumber != '" + travelTogethers.plateNumber + "' ")

    sb.append(" AND p.capturetime - t.capturetime  >= ").append((0 - travelTogethers.differ))
    sb.append(" AND p.capturetime - t.capturetime  <= ").append(travelTogethers.differ)

    if (travelTogethers.carYear != null && travelTogethers.carYear.size > 0 && travelTogethers.carYear(0) != "" && travelTogethers.carYear(0) != "0") {
      var m = travelTogethers.carYear.reduce((a, b) => a + "," + b)
      sb.append(" AND yearid IN (").append(m).append(")");
    }

    if (startTime != 0L) {
      sb.append(" AND p.capturetime >= ").append(startTime)
    }

    if (endTime != 0L) {
      sb.append(" AND p.capturetime <= ").append(endTime)
    }

    if (startTime != 0L) {
      sb.append(" AND dateid >= ").append(startTimeDateid)
    }

    if (endTime != 0L) {
      sb.append(" AND dateid <= ").append(endTimeDateid)
    }

    val carBrand = travelTogethers.carBrand

    val carModel = travelTogethers.carModel

    val carLevel = travelTogethers.carLevel

    val carColor = travelTogethers.carColor

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

    sb.append(" ) o ")

    sb.append(" GROUP BY  platenumber ")
    //    sb.append("  limit 10000 ")

    sb.append(" ) t ")
    sb.append(" WHERE t.total >= ").append(travelTogethers.count)

    sb.toString()
  }

}

//object TogetherCounter {
//
//  @volatile private var instance: LongAccumulator = null
//
//  def getInstance(sc: SparkContext): LongAccumulator = {
//    if (instance == null) {
//      synchronized {
//        if (instance == null) {
//          instance = sc.longAccumulator("TogetherCounter")
//        }
//      }
//    }
//    instance
//  }
//}


