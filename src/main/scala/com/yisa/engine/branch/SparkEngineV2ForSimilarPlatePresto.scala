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
import java.sql.Timestamp
import org.apache.spark.sql.Dataset
import java.text.SimpleDateFormat
import scala.collection.mutable.Map
import java.sql.DriverManager

class SparkEngineV2ForSimilarPlatePresto(line: String, tableName: String, zkHostport: String, PrestoHostPort: String) extends Runnable {

  override def run() {

    var line_arr = line.split("\\|")
    val job_id = line_arr(1)
    val line2 = line_arr(2).split(",")
    var model: String = null
    var bInsert: String = null
    var testSql: String = null

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val map: InputBean = gson.fromJson[InputBean](line_arr(2), mapType)

    val sql2 = getSql2(map, tableName)
    println("sql2:" + sql2)

    val count2 = map.count

    var PrestoURL = "jdbc:presto://" + PrestoHostPort + "/hive/yisadata"
    var PrestoUser = "spark"
    var PrestoPwd = ""
    var PrestoDriver = "com.facebook.presto.jdbc.PrestoDriver"

    Class.forName(PrestoDriver)
    val conn = DriverManager.getConnection(PrestoURL, PrestoUser, null)

    val stmt = conn.createStatement()

    val rs = stmt.executeQuery(sql2);

    val mysqlconn = MySQLConnectManager.getConnet(zkHostport)
    mysqlconn.setAutoCommit(false)
    //      val sql = "insert into xscpcb_progress(jobid,pronum,total) values(?,?,?)";
    val sql = "insert into xscpcb_result(j_id, s_id,y_id,count) values(?, ?, ?, ?)"
    val mysqlpstmt = mysqlconn.prepareStatement(sql);

    var count = 0

    while (rs.next()) {

      mysqlpstmt.setString(1, job_id);
      mysqlpstmt.setString(2, rs.getString(2));
      //          pstmt.setString(3, t(1).toString());
      mysqlpstmt.setString(3, rs.getInt(3) + "");
      mysqlpstmt.setString(4, rs.getInt(4) + "");
      mysqlpstmt.addBatch();
      count = count + 1
    }
    mysqlpstmt.executeBatch()

    val sqlProcess = "insert into xscpcb_progress(jobid,pronum,total) values(?,?,?)";
    val pstmtProcess = mysqlconn.prepareStatement(sqlProcess)

    if (count == 0) {
      count = -1
    }
    pstmtProcess.setString(1, job_id)
    pstmtProcess.setInt(2, 0)
    pstmtProcess.setInt(3, count)
    pstmtProcess.executeUpdate()
    pstmtProcess.close()

    mysqlconn.commit()
    mysqlpstmt.close()
    mysqlconn.close()

    rs.close();
    stmt.close()
    conn.close();

  }

  def getPlateNumber(headIndex: Int, length: Int, instr: String, n: Int, list: List[Int], res: List[String]): Unit =
    {
      var list2 = new ArrayList[Int]()
      list2.addAll(list)
      val len = instr.length() + length - n
      for (i <- headIndex until len) {

        if (length <= n) {
          //                res = res + inStr.substring(i, i + 1); 
          //                index = index+i;
          list.add(i);

          getPlateNumber(i + 1, length + 1, instr, n, list, res);
          if (length == n) {
            var array = instr.toCharArray();
            //                    System.out.println(res);
            //                    System.out.println(index);
            System.out.println(list.toString());
            if (!list.contains(0)) {
              //                    	System.out.println("---"+String.valueOf(array));
              for (t <- 0 until list.size()) {
                array(list.get(t)) = '_';
              }

              System.out.println("====" + String.valueOf(array));
              res.add(String.valueOf(array))
            }
          }
        } else {
          return
        }
        //            res = s;
        //            index = idx;
        list.clear();
        list.addAll(list2);

      }

    }

  def getSql2(param: InputBean, table: String): String = {
    val plateNumber = param.plateNumber
    val differ = param.differ
    var startTime: Long = 0
    var endTime: Long = 0
    if (param.startTime != null) {
      //      startTime = TimeUtil.getTimeStringFormat(param.startTime)
      startTime = getTimeStampSecond(param.startTime)
    }
    if (param.endTime != null) {
      //      endTime = TimeUtil.getTimeStringFormat(param.endTime)
      endTime = getTimeStampSecond(param.endTime)
    }
    //    val startTime = TimeUtil.getTimeStringFormat(param.startTime)
    //    val endTime = TimeUtil.getTimeStringFormat(param.endTime)
    val sql = new StringBuffer()
    //    sql.append("select plateNumber,solrid from "+table)
    sql.append("select platenumber,arbitrary(solrid) as solrid,arbitrary(yearid) as yearid,count(1) as count from " + table)
    sql.append(" where plateNumber != '' ")

    //各种另外的条件
    if (param.carYear != null && param.carYear.length != 0 && param.carYear(0) != "" && param.carYear(0) != "0") {
      var m = param.carYear.reduce((a, b) => a + "," + b)
      sql.append(" AND yearid IN (").append(m).append(")");
    }

    if (startTime != 0) {
      sql.append(" AND dateid >= ").append(getDateid(param.startTime))
    }

    if (endTime != 0) {
      sql.append(" AND dateid <= ").append(getDateid(param.endTime))
    }

    if (startTime != 0) {
      sql.append(" AND capturetime >= ").append(startTime)
    }

    if (endTime != 0) {
      sql.append(" AND capturetime <= ").append(endTime)
    }

    if (param.carModel != null && param.carModel.length > 0 && param.carModel(0) != "0") {
      val model = param.carModel
      var m = model.reduce((a, b) => a + "," + b)
      sql.append(" AND modelid IN (").append(m).append(")");
    }

    if (param.carBrand != null && param.carBrand != "" && param.carBrand != "0") {
      val brand = param.carBrand
      sql.append(" AND brandid = ").append(brand)
    }

    if (param.locationId != null && param.locationId.length != 0 && param.locationId(0) != "" && param.locationId(0) != "0") {
      var l = param.locationId.map { "'" + _ + "'" }.reduce((a, b) => a + "," + b)
      sql.append(" AND locationid IN (").append(l).append(")")
    }

    /////////////
    var list = new ArrayList[Int]()
    var plateList = new ArrayList[String]()
    getPlateNumber(0, 1, plateNumber, differ, list, plateList)
    if (plateList.size() > 0) {
      sql.append(" AND ( ")
      for (i <- 0 until plateList.size()) {
        if (0 == i) {
          sql.append(" plateNumber like '").append(plateList.get(i)).append("'")
        } else {
          sql.append(" OR plateNumber like '").append(plateList.get(i)).append("'")
        }
      }
      sql.append(" ) ")
    }

    sql.append(" group by platenumber limit 100 ")
    //    sql.append(" limit 100")
    sql.toString()

  }

  def getTimeSql(param: InputBean, table: String): String = {
    val plateNumber = param.plateNumber
    val differ = param.differ
    var startTime: Long = 0
    if (param.startTime != null) {
      //      startTime = TimeUtil.getTimeStringFormat(param.startTime)
      startTime = getTimeStampSecond(param.startTime)
    }
    var endTime: Long = 0
    if (param.endTime != null) {
      //      endTime = TimeUtil.getTimeStringFormat(param.endTime)
      endTime = getTimeStampSecond(param.endTime)
    }

    val sql = new StringBuffer()

    if (startTime != 0) {
      sql.append(" dateid >= ").append(getDateid(param.startTime))
    }

    if (endTime != 0) {
      sql.append(" AND dateid <= ").append(getDateid(param.endTime))
    }

    if (startTime != 0) {
      sql.append(" AND capturetime >= ").append(startTime)
    }

    if (endTime != 0) {
      sql.append(" AND capturetime <= ").append(endTime)
    }

    //    sql.append(") t where t.len = "+differ+" limit 100")
    sql.toString()

  }

  def getTimeStampSecond(time: String): Long = {
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    format.parse(time).getTime / 1000
  }

  def getDateid(timeString: String): String = {

    val timeLong = timeString.replaceAll("[-\\s:]", "")

    return timeLong.substring(0, 8);
  }

  def getTimeLong(timeString: String): Long = {
    val timeLong = timeString.replaceAll("[-\\s:]", "")

    return timeLong.substring(0, 14).toLong;
  }

}



