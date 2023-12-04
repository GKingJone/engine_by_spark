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
import java.sql.ResultSet
import java.sql.Connection
import java.sql.Statement
import java.sql.PreparedStatement

class SparkEngineV2ForMultiPointPresto(line: String, tableName: String, zkHostport: String, PrestoHostPort: String) extends Runnable {

  override def run() {

    // presto
    var PrestoURL = "jdbc:presto://" + PrestoHostPort + "/hive/yisadata"
    var PrestoUser = "spark"
    var PrestoPwd = ""
    var PrestoDriver = "com.facebook.presto.jdbc.PrestoDriver"

    var connP: Connection = null
    var stmtP: Statement = null
    var rsP: ResultSet = null

    // mysql
    var conn: Connection = null
    var sqlI = "insert into zdypz_result (s_id,j_id,t_id) values(?,?,?)"
    var pstmt: PreparedStatement = null

    var sql2 = "insert into zdypz_progress (jobid,total) values(?,?)"
    var pstmt2: PreparedStatement = null

    // 解析json
    var line_arr = line.split("\\|")

    val jobId = line_arr(1)
    val params = line_arr(2)

    val gson = new Gson
    val mapType = new TypeToken[Array[InputBean]] {}.getType
    val map: Array[InputBean] = gson.fromJson[Array[InputBean]](params, mapType)

    val map5 = new HashMap[Int, Int]()
    map5 += (1 -> 1)
    map5 += (2 -> 2)
    map5 += (3 -> 4)
    map5 += (4 -> 8)
    map5 += (5 -> 16)

    var inputBeanRepair: InputBean = null

    var resultData = null

    var count = 0

    var map16: HashMap[String, String] = get16

    var map31: HashMap[String, String] = get31

    // DB异常捕获
    try {

      // 判断条件是否为空
      if (map.length > 0) {

        // 否定条件的
        var isType: String = ""
        var isTypeShow: String = ""

        var sql: StringBuffer = new StringBuffer()

        sql.append("select max_by(solrid,capturetime) as solrid,sum(type) as types from (")

        var i: Int = 1

        map.foreach { m =>

          // 获取否定条件的条件ID
          if (m.isRepair == 1) {

            isTypeShow = m.differ.toString()

            isType = map5.get(m.differ).get.toString()

            map16 = map16.filter(!_._2.contains(isType))
          }

          count = m.count

          var sqlTem = gsonArrayFunc(m, tableName, map5)

          sql.append("(").append(sqlTem).append(")")

          if (i < map.length) {

            sql.append(" union all ")

            i = i + 1

          }

        }

        sql.append(")  group by platenumber order by types desc ")

        println("sql-all : " + sql.toString())

        var number = 0

        conn = MySQLConnectManager.getConnet(zkHostport)
        conn.setAutoCommit(false)
        pstmt = conn.prepareStatement(sqlI)
        pstmt2 = conn.prepareStatement(sql2)

        Class.forName(PrestoDriver)
        connP = DriverManager.getConnection(PrestoURL, PrestoUser, null)
        stmtP = connP.createStatement()
        rsP = stmtP.executeQuery(sql.toString());

        // 非否定条件的场合
        if ("".equals(isType)) {

          // 插入sql
          while (rsP.next()) {

            var solrid = rsP.getString(1)
            var types = rsP.getString(2)

            if (count >= 2) {
              if (count > 2) {
                count = count - 1
              }

              var lenTem = map31.get(types).toString().split(",").size

              if (lenTem >= count) {

                number = number + 1

                pstmt.setString(1, solrid);
                pstmt.setString(2, jobId);
                pstmt.setString(3, map31.get(types).get);
                pstmt.addBatch();

                // 每500条更新一次
                if (number % 500 == 0) {

                  pstmt.executeBatch()

                }

              }
            }

          }

          // 更新进度
          if (number == 0) {
            number = -1
          }

          pstmt2.setString(1, jobId);
          pstmt2.setInt(2, number);
          pstmt2.executeUpdate()

          pstmt.executeBatch()
          conn.commit()

        } else {
          // 否定条件的场合

          // 插入sql
          while (rsP.next()) {

            var solrid = rsP.getString(1)
            var types = rsP.getString(2)

            // 去除否定条件的数据
            if (map16.contains(types)) {

              if (count >= 2) {
                if (count > 2) {
                  count = count - 1
                }

                var lenTem = map31.get(types).toString().split(",").size

                if (lenTem >= count) {

                  number = number + 1

                  pstmt.setString(1, solrid);
                  pstmt.setString(2, jobId);
                  pstmt.setString(3, map31.get(types).get + "," + isTypeShow);
                  pstmt.addBatch();

                  // 每500条更新一次
                  if (number % 500 == 0) {

                    pstmt.executeBatch()

                  }

                }
              }

            }
          }

          // 更新进度

          if (number == 0) {
            number = -1
          }

          pstmt2.setString(1, jobId);
          pstmt2.setInt(2, number);
          pstmt2.executeUpdate()

          pstmt.executeBatch()
          conn.commit()

        }

      } else {

        println("参数异常 ：" + line_arr)

      }

    } catch {

      case ex: Exception => {
        println("多点碰撞的presto异常 ： " + ex.printStackTrace())

        pstmt2.setString(1, jobId);
        pstmt2.setInt(2, -1);
        pstmt2.executeUpdate()
      }

    } finally {

      if (pstmt2 != null) {
        pstmt2.close()

        println("pstmt2正常关闭！")
      }

      if (pstmt != null) {
        pstmt.close()

        println("pstmt正常关闭！")
      }

      if (conn != null) {
        conn.close()

        println("conn正常关闭！")
      }

      if (rsP != null) {
        rsP.close()

        println("rsP正常关闭！")
      }

      if (stmtP != null) {
        stmtP.close()

        println("stmtP正常关闭！")
      }

      if (connP != null) {
        connP.close()

        println("connP正常关闭！")
      }
    }
  }

  def gsonArrayFunc(inputBean: InputBean, tableName: String, map5: HashMap[Int, Int]): String = {

    var startTime1 = TimeUtil.getTimeStringFormat(inputBean.startTime)
    var endTime1 = TimeUtil.getTimeStringFormat(inputBean.endTime)
    var locationId: Array[String] = inputBean.locationId
    var carModel: Array[String] = inputBean.carModel
    var carBrand: String = inputBean.carBrand
    var carYear: Array[String] = inputBean.carYear
    var carColor: String = inputBean.carColor
    var plateNumber: String = inputBean.plateNumber
    var differ: Int = inputBean.differ
    var direction:String = inputBean.direction
    var carLevel: Array[String] = inputBean.carLevel

    val format2 = new SimpleDateFormat("yyyyMMddHHmmss")

    var startTime2 = format2.parse(inputBean.startTime).getTime / 1000
    var endTime2 = format2.parse(inputBean.endTime).getTime / 1000

    var sb = new StringBuffer()

    sb.append(" SELECT  max(capturetime) as capturetime, max_by(solrid,capturetime) as solrid, platenumber , ")

    sb.append(map5.get(differ).get).append(" as type from ").append(tableName)

    if (startTime1 != null) {
      sb.append(" WHERE dateid >= ").append(getDateid(startTime1))
    }

    if (endTime1 != null) {
      sb.append(" AND dateid <= ").append(getDateid(endTime1))
    }

    if (startTime2 != 0) {
      sb.append(" AND capturetime >= ").append(startTime2)
    }

    if (endTime2 != 0) {
      sb.append(" AND capturetime <= ").append(endTime2)
    }

    if (locationId != null && locationId.length > 0) {
      var l = locationId.map { "'" + _ + "'" }.reduce((a, b) => a + "," + b)
      sb.append(" AND locationid IN (").append(l).append(")")
    }
    
    if (carLevel != null && carLevel.length > 0) {
      var m = carLevel.reduce((a, b) => a + "," + b)
      sb.append(" AND levelid IN (").append(m).append(")");
    }

    if (carYear != null && carYear.length > 0) {
      var m = carYear.reduce((a, b) => a + "," + b)
      sb.append(" AND yearid IN (").append(m).append(")");
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

    sb.append(" group by platenumber")

    println("sql : " + sb.toString())

    sb.toString()

  }

  def get16(): HashMap[String, String] = {

    val map16 = new HashMap[String, String]()
    map16 += ("1" -> "1")
    map16 += ("2" -> "2")
    map16 += ("3" -> "1,2")
    map16 += ("4" -> "4")
    map16 += ("5" -> "1,4")
    map16 += ("6" -> "2,4")
    map16 += ("7" -> "1,2,4")
    map16 += ("8" -> "8")
    map16 += ("9" -> "1,8")
    map16 += ("10" -> "2,8")
    map16 += ("11" -> "1,2,8")
    map16 += ("12" -> "4,8")
    map16 += ("13" -> "1,4,8")
    map16 += ("14" -> "2,4,8")
    map16 += ("15" -> "1,2,4,8")
    map16 += ("16" -> "16")
    map16 += ("17" -> "1,16")
    map16 += ("18" -> "2,16")
    map16 += ("19" -> "1,2,16")
    map16 += ("20" -> "4,16")
    map16 += ("21" -> "1,4,16")
    map16 += ("22" -> "2,4,16")
    map16 += ("23" -> "1,2,4,16")
    map16 += ("24" -> "8,16")
    map16 += ("25" -> "1,8,16")
    map16 += ("26" -> "2,8,16")
    map16 += ("27" -> "1,2,8,16")
    map16 += ("28" -> "4,8,16")
    map16 += ("29" -> "1,4,8,16")
    map16 += ("30" -> "2,4,8,16")
    map16 += ("31" -> "1,2,4,8,16")

    map16

  }

  def get31(): HashMap[String, String] = {

    val map16 = new HashMap[String, String]()
    map16 += ("1" -> "1")
    map16 += ("2" -> "2")
    map16 += ("3" -> "1,2")
    map16 += ("4" -> "3")
    map16 += ("5" -> "1,3")
    map16 += ("6" -> "2,3")
    map16 += ("7" -> "1,2,3")
    map16 += ("8" -> "4")
    map16 += ("9" -> "1,4")
    map16 += ("10" -> "2,4")
    map16 += ("11" -> "1,2,4")
    map16 += ("12" -> "3,4")
    map16 += ("13" -> "1,3,4")
    map16 += ("14" -> "2,3,4")
    map16 += ("15" -> "1,2,3,4")
    map16 += ("16" -> "5")
    map16 += ("17" -> "1,5")
    map16 += ("18" -> "2,5")
    map16 += ("19" -> "1,2,5")
    map16 += ("20" -> "3,5")
    map16 += ("21" -> "1,3,5")
    map16 += ("22" -> "2,3,5")
    map16 += ("23" -> "1,2,3,5")
    map16 += ("24" -> "4,5")
    map16 += ("25" -> "1,4,5")
    map16 += ("26" -> "2,4,5")
    map16 += ("27" -> "1,2,4,5")
    map16 += ("28" -> "3,4,5")
    map16 += ("29" -> "1,3,4,5")
    map16 += ("30" -> "2,3,4,5")
    map16 += ("31" -> "1,2,3,4,5")

    map16

  }

  def getDateid(timeString: String): String = {

    val timeLong = timeString.replaceAll("[-\\s:]", "")

    return timeLong.substring(0, 8);
  }
}