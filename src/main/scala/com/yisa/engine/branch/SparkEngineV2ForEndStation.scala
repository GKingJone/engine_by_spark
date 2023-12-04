package com.yisa.engine.branch

import org.apache.spark.sql.SparkSession
import com.yisa.engine.common.InputBean
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.spark.util.LongAccumulator
import org.apache.spark.SparkContext
import com.yisa.engine.db.MySQLConnectManager
import java.util.ArrayList
import java.util.List
import com.yisa.engine.uitl.TimeUtil
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.hadoop.yarn.webapp.view.HtmlPage._
import java.text.SimpleDateFormat
import java.util.Calendar

class SparkEngineV2ForEndStation {

  def searchEndStation(sparkData: Dataset[Row], sqlContext: SparkSession, line: String, tableName: String, zkHostport: String): Unit = {

    var conn = MySQLConnectManager.getConnet(zkHostport)
    conn.setAutoCommit(false)
    var sql = "insert into zjwjz_result (s_id,j_id,t_n,l_c) values(?,?,?,?)";
    var pstmt = conn.prepareStatement(sql);

    var line_arr = line.split("\\|")

    val jobId = line_arr(1)
    val params = line_arr(2)

    val gson = new Gson
    val mapType = new TypeToken[Array[InputBean]] {}.getType
    val map: Array[InputBean] = gson.fromJson[Array[InputBean]](params, mapType)

    var inputBeanRepair: InputBean = null

    var resultData = null

    var count = 0

    // 判断条件是否为空
    if (map.length > 0) {

      var i: Int = 1
      var df: Dataset[Row] = null

      map.foreach { m =>

        //        val format2 = new SimpleDateFormat("yyyyMMddHHmmss")
        //
        //        var startTime = format2.parse(m.startTime).getTime / 1000
        //        var endTime = format2.parse(m.endTime).getTime / 1000

        if (i == 1) {
          df = sqlContext.sql(gsonArrayFunc(m, i + "", tableName))
          //            .filter("capturetime >= " + startTime)
          //            .filter("capturetime <= " + endTime)

          i = i + 1

        } else {

          var dfTemp = sqlContext.sql(gsonArrayFunc(m, i + "", tableName))
          //            .filter("capturetime >= " + startTime)
          //            .filter("capturetime <= " + endTime)

          df = df.union(dfTemp)

          i = i + 1
        }

      }

      var dfIs: Dataset[Row] = null
      if (df != null) {

        df.createOrReplaceTempView("df")

        var sqlUnion = "SELECT first(solrid) as solrid, count(1) as number,first(lastcaptured) as lastcaptured from df group by platenumber"
        dfIs = sqlContext.sql(sqlUnion)

        dfIs.createOrReplaceTempView("dfIs")

        var number = 0
        dfIs.collect().foreach { data =>

          number = number + 1

          pstmt.setString(1, data(0).toString());
          pstmt.setString(2, jobId);
          pstmt.setInt(3, data(1).toString().toInt);
          pstmt.setInt(4, data(2).toString().toInt);
          pstmt.addBatch();

        }

        var sql2 = "insert into zjwjz_progress (jobid,total) values(?,?)";
        var pstmt2 = conn.prepareStatement(sql2);

        if (number == 0) {
          number = -1
        }
        pstmt2.setString(1, jobId);
        pstmt2.setInt(2, number);
        pstmt2.executeUpdate()
        pstmt2.close()

        pstmt.executeBatch()
        conn.commit()
        pstmt.close()
        conn.close()

      }

    }
  }

  def getOldDay(): String = {

    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -3)
    var yesterday = dateFormat.format(cal.getTime())

    yesterday

  }

  def gsonArrayFunc(inputBean: InputBean, typeC: String, tableName: String): String = {

    //    val jobId: String = map.jobId

    var startTime1 = TimeUtil.getTimeStringFormat(inputBean.startTime)
    var endTime1 = TimeUtil.getTimeStringFormat(inputBean.endTime)
    var locationId: Array[String] = inputBean.locationId
    var plateNumber: String = inputBean.plateNumber

    val format2 = new SimpleDateFormat("yyyyMMddHHmmss")

    var startTime = format2.parse(inputBean.startTime).getTime / 1000
    var endTime = format2.parse(inputBean.endTime).getTime / 1000

    var sb = new StringBuffer()

    sb.append(" SELECT  first(lastcaptured) as lastcaptured,first(solrid) as solrid, platenumber , '")

    sb.append(typeC).append("' as type from ").append(tableName)

    sb.append(" WHERE dateid >= ").append(getDateid(startTime1))

    sb.append(" AND dateid <= ").append(getDateid(endTime1))

    sb.append(" AND capturetime >= ").append(startTime)

    sb.append(" AND capturetime <= ").append(endTime)

    if (locationId != null && locationId.length > 0) {
      var l = locationId.map { "'" + _ + "'" }.reduce((a, b) => a + "," + b)
      //   		  var l = locationId.map { "" + _ + "" }.reduce((a, b) => a + "," + b)
      sb.append(" AND locationid IN (").append(l).append(")")
    }

    if (plateNumber != null && plateNumber != "") {
      sb.append(" AND plateNumber NOT LIKE '%").append(plateNumber.replace("*", "%").replace("?", "_")).append("%'")
    }

    sb.append(" group by platenumber ")

    println(sb.toString())

    sb.toString()

  }

  def getDateid(timeString: String): String = {

    val timeLong = timeString.replaceAll("[-\\s:]", "")

    return timeLong.substring(0, 8);
  }
}