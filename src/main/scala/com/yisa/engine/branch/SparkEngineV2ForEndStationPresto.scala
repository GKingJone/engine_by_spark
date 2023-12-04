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
import java.sql.DriverManager
import scala.collection.mutable.HashMap

class SparkEngineV2ForEndStationPresto(line: String, tableName: String, zkHostport: String, PrestoHostPort: String) extends Runnable {

  override def run() {

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

      var sql: StringBuffer = new StringBuffer()

      sql.append("select max(lastcaptured) as lastcaptured,arbitrary(solrid) as solrid,count(1) as num from (")

      var i: Int = 1

      map.foreach { m =>

        count = m.count

        var sqlTem = gsonArrayFunc(m, tableName)

        sql.append("(").append(sqlTem).append(")")

        if (i < map.length) {

          sql.append(" union all ")

          i = i + 1

        }

      }

      sql.append(")  group by platenumber")

      println("sql-all : " + sql.toString())

      var PrestoURL = "jdbc:presto://" + PrestoHostPort + "/hive/yisadata"
      var PrestoUser = "spark"
      var PrestoPwd = ""
      var PrestoDriver = "com.facebook.presto.jdbc.PrestoDriver"

      Class.forName(PrestoDriver)
      val connP = DriverManager.getConnection(PrestoURL, PrestoUser, null)

      val stmtP = connP.createStatement()

      val rsP = stmtP.executeQuery(sql.toString());

      var conn = MySQLConnectManager.getConnet(zkHostport)
      conn.setAutoCommit(false)
      var sqlI = "insert into zjwjz_result (s_id,j_id,t_n,l_c) values(?,?,?,?)";
      var pstmt = conn.prepareStatement(sqlI);

      var number = 0

      // 插入sql
      while (rsP.next()) {

        var lastCaptured = rsP.getString(1)
        var solrid = rsP.getString(2)
        var num = rsP.getString(3)

        number = number + 1

        pstmt.setString(1, solrid);
        pstmt.setString(2, jobId);
        pstmt.setString(3, num);
        pstmt.setString(4, lastCaptured);
        pstmt.addBatch();

        // 每500条更新一次
        if (number % 500 == 0) {

          pstmt.executeBatch()

        }

      }

      // 更新进度
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
      
      rsP.close()
      stmtP.close()
      connP.close()
      
      

    } else {

      println("参数异常 ：" + line_arr)
    }
  }

  def gsonArrayFunc(inputBean: InputBean, tableName: String): String = {

    //    val jobId: String = map.jobId

    var startTime1 = TimeUtil.getTimeStringFormat(inputBean.startTime)
    var endTime1 = TimeUtil.getTimeStringFormat(inputBean.endTime)
    var locationId: Array[String] = inputBean.locationId
    var plateNumber: String = inputBean.plateNumber
    var differ: Int = inputBean.differ

    val format2 = new SimpleDateFormat("yyyyMMddHHmmss")

    var startTime = format2.parse(inputBean.startTime).getTime / 1000
    var endTime = format2.parse(inputBean.endTime).getTime / 1000

    var sb = new StringBuffer()

    sb.append(" SELECT  max(lastcaptured) as lastcaptured,arbitrary(solrid) as solrid,  ")

    sb.append(" platenumber from ").append(tableName)

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