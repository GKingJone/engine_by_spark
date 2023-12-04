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
import org.apache.spark.sql.functions._

class SparkEngineV2ForMultiPoint {

  def searchMultiPoint(sparkData: Dataset[Row], sqlContext: SparkSession, line: String, tableName: String, zkHostport: String): Unit = {

    var line_arr = line.split("\\|")

    val jobId = line_arr(1)
    val params = line_arr(2)

    val gson = new Gson
    val mapType = new TypeToken[Array[InputBean]] {}.getType
    val map: Array[InputBean] = gson.fromJson[Array[InputBean]](params, mapType)

    var inputBeanRepair: InputBean = null

    var resultData = null

    var count = 0
    var isErr = 0

    // 判断条件是否为空
    if (map.length > 0) {

      var i: Int = 1
      var df: Dataset[Row] = null
      var dfNot: Dataset[Row] = null

      var sqlAll = new StringBuffer();

      map.foreach { m =>

        if (m.isRepair == 1) {
          inputBeanRepair = m

          if (inputBeanRepair != null) {
            dfNot = sqlContext.sql(gsonArrayFunc(inputBeanRepair, i + "", tableName));
  
            df = df.union(dfNot)
          }
          
          isErr = i;
          
          i = i + 1;
          
        } else {

          val format2 = new SimpleDateFormat("yyyyMMddHHmmss")

          var startTime = format2.parse(m.startTime).getTime / 1000
          var endTime = format2.parse(m.endTime).getTime / 1000

          if (i == 1) {
            df = sqlContext.sql(gsonArrayFunc(m, i + "", tableName))

            i = i + 1

            count = m.count

          } else {

            var dfTemp = sqlContext.sql(gsonArrayFunc(m, i + "", tableName))

            df = df.union(dfTemp)

            i = i + 1
          }

        }

      }

      var dfIs: Dataset[Row] = null
      if (df != null) {

        df.createOrReplaceTempView("df")

        var countSql = 2;
        if (count > 2) {
          countSql = count - 1
        }

        val sqlUnion = new StringBuffer();
        sqlUnion.append("select max_by(solrid,capturetime) as solrid,platenumber,concat_ws(',',collect_set(type)) as types,size(collect_set(type)) as typesize from df group by platenumber ")
        sqlUnion.append(" HAVING typesize >= " + countSql)

        val sqlUnionS = sqlUnion.toString()

        println("sql : " + sqlUnion.toString())

        dfIs = sqlContext.sql(sqlUnionS)

        if (inputBeanRepair != null) {
          
          println("排除条件！")

          dfIs.createOrReplaceTempView("dfIs")

          var sqlNot = "SELECT solrid, types FROM dfIs WHERE types NOT LIKE '%" + isErr + "%'"

          println( "SELECT solrid, types FROM dfIs WHERE types NOT LIKE '%" + isErr + "%'")
          
          var re = sqlContext.sql(sqlNot);

          var conn = MySQLConnectManager.getConnet(zkHostport)
          conn.setAutoCommit(false)
          var sql = "insert into zdypz_result (s_id,j_id,t_id) values(?,?,?)";
          var pstmt = conn.prepareStatement(sql);

          var number = 0

          val reCollect = re.collect()

          println("reCollect count:" + reCollect.length)

          reCollect.foreach { t =>

            number = number + 1

            pstmt.setString(1, t(0).toString());
            pstmt.setString(2, jobId);
            pstmt.setString(3, t(1).toString() + "," + isErr);
            pstmt.addBatch();

          }

          var sql2 = "insert into zdypz_progress (jobid,total) values(?,?)";
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
        } else {

          //          if (count >= 2) {
          //
          //            if (count > 2) {
          //              count = count - 1
          //            }
          //
          //            dfIs = dfIs.filter { x => x.getAs[String]("types").split(",").length >= count }
          //
          //          }

          //          println("dfIs filter count" + dfIs.count())

          var conn = MySQLConnectManager.getConnet(zkHostport)
          conn.setAutoCommit(false)
          var sql = "insert into zdypz_result (s_id,j_id,t_id) values(?,?,?)";
          var pstmt = conn.prepareStatement(sql);

          var number = 0;
          dfIs.collect().foreach { t =>

            number = number + 1

            pstmt.setString(1, t(0).toString());
            pstmt.setString(2, jobId);
            pstmt.setString(3, t(2).toString());
            pstmt.addBatch();

          }

          var sql2 = "insert into zdypz_progress (jobid,total) values(?,?)";
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
    var carModel: Array[String] = inputBean.carModel
    var carBrand: String = inputBean.carBrand
    var carYear: Array[String] = inputBean.carYear
    var carColor: String = inputBean.carColor
    var plateNumber: String = inputBean.plateNumber
    var carLevel:Array[String] = inputBean.carLevel

    val format2 = new SimpleDateFormat("yyyyMMddHHmmss")

    var startTime2 = format2.parse(inputBean.startTime).getTime / 1000
    var endTime2 = format2.parse(inputBean.endTime).getTime / 1000
    
    var direction: String = inputBean.direction

    var sb = new StringBuffer()

    sb.append(" SELECT  solrid, capturetime, platenumber , '")

    //    sb.append(" SELECT solrid, capturetime, platenumber , '")
    sb.append(typeC).append("' as type from ").append(tableName)

    if (startTime2 != 0) {
      sb.append(" WHERE capturetime >= ").append(startTime2)
    }

    if (endTime2 != 0) {
      sb.append(" AND capturetime <= ").append(endTime2)
    }

    if (startTime1 != null) {
      sb.append(" AND dateid >= ").append(getDateid(startTime1))
    }

    if (endTime1 != null) {
      sb.append(" AND dateid <= ").append(getDateid(endTime1))
    }

    if (locationId != null && locationId.length > 0) {
      var l = locationId.map { "'" + _ + "'" }.reduce((a, b) => a + "," + b)
      //   		  var l = locationId.map { "" + _ + "" }.reduce((a, b) => a + "," + b)
      sb.append(" AND locationid IN (").append(l).append(")")
    }

    if (carYear != null && carYear.length > 0) {
      var m = carYear.reduce((a, b) => a + "," + b)
      sb.append(" AND yearid IN (").append(m).append(")");
    }
    
    if (carLevel != null && carLevel.length > 0) {
      var m = carLevel.reduce((a, b) => a + "," + b)
      sb.append(" AND levelid IN (").append(m).append(")");
    }

    if (carModel != null && carModel.length > 0) {
      var m = carModel.reduce((a, b) => a + "," + b)
      sb.append(" AND modelid IN (").append(m).append(")");
    }

    if (direction != null && direction != "") {
      sb.append(" AND direction = '").append(direction).append("'")
    }

    if (carBrand != null && carBrand != "") {
      sb.append(" AND brandid = ").append(carBrand)
    }

    if (carColor != null && carColor != "") {
      sb.append(" AND colorId = ").append(carColor)
    }

    if (plateNumber != null && plateNumber != "") {
      if (plateNumber.contains("*") || plateNumber.contains("?")) {
        sb.append(" AND plateNumber LIKE '%").append(plateNumber.replace("*", "%").replace("?", "_")).append("%'")
      } else {
        sb.append(" AND plateNumber = '").append(plateNumber).append("'")
      }
    }

    //        sb.append(" order by capturetime desc ")

    println(sb.toString())

    sb.toString()

  }

  def getDateid(timeString: String): String = {

    val timeLong = timeString.replaceAll("[-\\s:]", "")

    return timeLong.substring(0, 8);
  }
}