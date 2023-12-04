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
class SparkEngineV2ForTravelTogether {

  def TravelTogether(sparkSession: SparkSession, line: String, tableName: String, zkHostport: String): Unit = {

    val jdbcTable = "togetherCar_result"

    var line_arr = line.split("\\|")

    val jobId = line_arr(1)
    val params = line_arr(2)

    println(params)

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val travelTogether: InputBean = gson.fromJson[InputBean](params, mapType)

    val countT = travelTogether.count

    val tmpTable = registTmpTable(travelTogether, sparkSession, tableName)
    val pass_info_tmp_all = registTmpAllTable(travelTogether, sparkSession, tableName,tmpTable)

    val sqlStr = getSQL(travelTogether, params,pass_info_tmp_all, tmpTable)
    println("SQL--------------:" + sqlStr)

    val resultData = sparkSession.sql(sqlStr)
    resultData.foreachPartition { data =>

      //      val droppedWordsCounter = TogetherCounter.getInstance(sparkSession.sparkContext)

      var conn = MySQLConnectManager.getConnet(zkHostport)
      conn.setAutoCommit(false)
      val sql = new StringBuffer()

      sql.append("insert into ").append(jdbcTable)
      sql.append(" (")
      sql.append("j_id,s_id,plate_number,days,locationid_detail,locationid_count,total,is_sheltered,last_captured,yearid")
      sql.append(")").append("values(?,?,?,?,?,?,?,?,?,?)");
      var pstmt = conn.prepareStatement(sql.toString());
      //      var count = 0

      //    sb.append(" SELECT concat_ws(',',collect_set(p.solrid )) AS solrids ,platenumber,")
      //    sb.append(" size(collect_set(p.dateid )) AS days,collect_list(p.locationid) AS locationids, ")
      //    sb.append(" count(p.solrid) AS total,sum(p.issheltered) AS issheltered,max(p.lastcaptured) AS lastcaptured,")
      //    sb.append(" first(yearid) AS yearid  ")
      data.foreach { t =>
        {

          var gson = new Gson()
          val total = t.getLong(4);

          //准备Locationid
          //将locationid放到一个map集合，value为出现次数
          var locationids = t.getList[String](3)
          var lt = locationids.iterator()
          var locationid_m = Map[String, Int]()
          while (lt.hasNext()) {
            val locationid = lt.next();
            if (locationid_m.contains(locationid)) {
              locationid_m = locationid_m.+(locationid -> (locationid_m.get(locationid).get + 1))
            } else {
              locationid_m = locationid_m.+(locationid -> 1)
            }
          }

          val locationid_detail_List = new ArrayList[Locationid_detail]();
          val locationid_count = locationid_m.size

          locationid_m.foreach(e => {
            val (k, v) = e

            val locationid_detail = Locationid_detail.apply(k, v);
            locationid_detail_List.add(locationid_detail)

          })

          pstmt.setString(1, jobId);
          pstmt.setString(2, t(0).toString());
          pstmt.setString(3, t(1).toString());
          pstmt.setInt(4, t.getInt(2));
          pstmt.setString(5, gson.toJson(locationid_detail_List));
          pstmt.setInt(6, locationid_count);
          pstmt.setInt(7, (total + "").toInt);
          pstmt.setInt(8, t.getInt(5));
          if (t.getInt(6) != null) {
            pstmt.setInt(9, t.getInt(6));
          } else {
            pstmt.setInt(9, 0);
          }
          if (t.getInt(7) != null) {
            pstmt.setInt(10, t.getInt(7));
          } else {
            pstmt.setInt(10, -1);
          }

          pstmt.addBatch();
          //          droppedWordsCounter.add(1)
          //          count += 1

        }
      }

      //      var sql2 = "insert into togetherCar_count (j_id,count) values(?,?)";
      //      var pstmt2 = conn.prepareStatement(sql2);

      //      if (count == 0) {
      //        count = -1
      //      }

      //      pstmt2.setString(1, jobId);
      //      pstmt2.setInt(2, count);
      //      pstmt2.executeUpdate()
      //      pstmt2.close()

      pstmt.executeBatch()
      conn.commit()
      pstmt.close()
      conn.close()

    }
    val conn = MySQLConnectManager.getConnet(zkHostport)
    val query = "select * from " + jdbcTable + " where j_id = '" + jobId + "' limit 1   ";
    val stmt3 = conn.prepareStatement(query);
    val sql2 = "insert into togetherCar_count (j_id,count) values(?,?)";
    val pstmt2 = conn.prepareStatement(sql2);
    try {
      pstmt2.setString(1, jobId)
      if (stmt3.executeQuery().next()) {

        pstmt2.setInt(2, 1)

      } else {
        pstmt2.setInt(2, -1)
      }
      pstmt2.executeUpdate()
    } catch {
      case e: Exception => {
        println("task wrong!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        println(e.getMessage)
        println(e.printStackTrace())
      }
    } finally {

      pstmt2.close()
      stmt3.close()
      conn.close()

    }

  }

  def registTmpAllTable(travelTogethers: InputBean, sparkSession: SparkSession, tableName: String, locationidsTable: String): String = {

    val startTimeDateid = TimeUtil.getDateId(travelTogethers.startTime)
    val endTimeDateid = TimeUtil.getDateId(travelTogethers.endTime)

    val sql = new StringBuffer();
    sql.append("SELECT *  ").append(" FROM ").append(tableName);
    sql.append(" WHERE ")

    sql.append("  capturetime   >= ").append(TimeUtil.getTimestampLong((travelTogethers.startTime)));
    sql.append(" AND capturetime   <= ").append(TimeUtil.getTimestampLong((travelTogethers.endTime)));

    sql.append(" AND locationid IN (").append(" select locationid from ").append(locationidsTable).append(")")

    sql.append(" AND dateid >= ").append(startTimeDateid)

    sql.append(" AND dateid <= ").append(endTimeDateid)

    val tmpTable = "pass_info_tmp_all";

    val resultData_1 = sparkSession.sql(sql.toString());

    println(sql.toString())

    resultData_1.createOrReplaceTempView(tmpTable);

    return tmpTable;

  }

  def registTmpTable(travelTogethers: InputBean, sparkSession: SparkSession, tableName: String): String = {

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

    val tmpTable = "locationidsTable";

    val resultData_1 = sparkSession.sql(sql.toString());

    println(sql.toString())

    resultData_1.createOrReplaceTempView(tmpTable);

    return tmpTable;

  }

  def getSQL(travelTogethers: InputBean, js: String, tableName: String, tmpTable: String): String = {

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
    sb.append(" max(o.yearid) AS yearid  ")

    sb.append(" FROM  ( ")

    sb.append(" SELECT distinct p.solrid,p.platenumber,p.dateid,p.locationid,p.solrid,p.issheltered,p.lastcaptured,p.yearid")

    sb.append(" FROM ").append(tableName).append(" p , ").append(tmpTable).append(" t ")

    //    sb.append(" JOIN  ").append(" ON p.locationid = t.locationid ")
    sb.append(" WHERE  ").append("  p.locationid = t.locationid ")

    sb.append(" AND   platenumber not like  '%无%' AND  platenumber not like  '%未%'  AND  platenumber not like  '%牌%'  AND  platenumber not like  '%?%'  AND  platenumber not like  '%00000%'  ")

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

    sb.append("  WHERE   platenumber not like  '%无%' AND  platenumber not like  '%未%'  AND  platenumber not like  '%牌%'  AND  platenumber not like  '%?%'  AND  platenumber not like  '%00000%'  ")

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


