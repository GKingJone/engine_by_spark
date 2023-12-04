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

class SparkEngineV2ForExportLocation {

  def searchExportLocation(sparkData: Dataset[Row], sqlContext: SparkSession, line: String, tableName: String, zkHostport: String): Unit = {

    var conn = MySQLConnectManager.getConnet(zkHostport)
    conn.setAutoCommit(false)
    var sql = "insert into el_result (l_id,j_id) values(?,?)";
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

        val format2 = new SimpleDateFormat("yyyyMMddHHmmss")

        var startTime = format2.parse(m.startTime).getTime / 1000
        var endTime = format2.parse(m.endTime).getTime / 1000

        df = sqlContext.sql(gsonArrayFunc(m, i + "", tableName))

      }

      if (df != null) {

        var number = 0
        df.collect().foreach { data =>

          number = number + 1

          pstmt.setString(1, data(0).toString());
          pstmt.setString(2, jobId);
          pstmt.addBatch();
        }

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
    var carModel: Array[String] = inputBean.carModel
    var carBrand: String = inputBean.carBrand
    var carYear: Array[String] = inputBean.carYear
    var carColor: String = inputBean.carColor
    var plateNumber: String = inputBean.plateNumber

    var sb = new StringBuffer()

    sb.append(" SELECT  first(locationId) as locationId  from ").append(tableName)

    if (startTime1 != null) {
      sb.append(" WHERE dateid >= ").append(getDateid(startTime1))
    }

    if (endTime1 != null) {
      sb.append(" AND dateid <= ").append(getDateid(endTime1))
    }
    
    sb.append(" group by locationId")

    println(sb.toString())

    sb.toString()

  }

  def getDateid(timeString: String): String = {

    val timeLong = timeString.replaceAll("[-\\s:]", "")

    return timeLong.substring(0, 8);
  }
}