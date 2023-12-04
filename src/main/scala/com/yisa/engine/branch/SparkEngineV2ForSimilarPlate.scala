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

class SparkEngineV2ForSimilarPlate {

  def searchSimilarPlateNumber(sparkData: Dataset[Row], sqlContext: SparkSession, line: String, tableName: String, zkHostport: String): Unit = {

    var line_arr = line.split("\\|")
    val job_id = line_arr(1)
    val line2 = line_arr(2).split(",")
    var model: String = null
    var bInsert: String = null
    var testSql: String = null
    if (line_arr.length > 3) {
      model = line_arr(3)
      bInsert = line_arr(4)
      testSql = line_arr(5)
    }
    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val map: InputBean = gson.fromJson[InputBean](line_arr(2), mapType)

    val begintime = System.currentTimeMillis()

    val sql = getSql(map, tableName)
    val timeSql = getTimeSql(map, tableName)

    println("new自定义函数sql查询语句为:" + sql)
    println("new时间sql:" + timeSql)
    var result: Dataset[Row] = null
    if (timeSql != null && !timeSql.isEmpty()) {
      result = sqlContext.sql(sql).filter(timeSql).limit(10000)
    } else {
      //         result = sqlContext.sql(sql).limit(1000)
      result = sqlContext.sql(sql).limit(10000)
    }

    if (model == null) {
      var mapRes: Map[String, Int] = Map()
      val conn = MySQLConnectManager.getConnet(zkHostport)
      conn.setAutoCommit(false)
      val sqlProcess = "insert into xscpcb_progress(jobid,pronum,total) values(?,?,?)";
      val sql2 = "insert into xscpcb_result(j_id, s_id, y_id,count) values(?, ?, ?, ?)"
      val pstmt = conn.prepareStatement(sql2);
      val pstmtProcess = conn.prepareStatement(sqlProcess)

      result.collect().foreach { t =>
        //有的就不再入库了
        if (mapRes.contains(t(0).toString())) {

        } else {
          mapRes.put(t(0).toString(), 1)
          pstmt.setString(1, job_id);
          pstmt.setString(2, t(1).toString());
          pstmt.setString(3, t(2) + "");
          pstmt.setString(4, t(3).toString());
          pstmt.addBatch();
        }

      }

      if (mapRes.size == 0) {
        pstmtProcess.setString(1, job_id)
        pstmtProcess.setInt(2, 0)
        pstmtProcess.setInt(3, -1)
        
      } else {
        pstmtProcess.setString(1, job_id)
        pstmtProcess.setInt(2, 0)
        pstmtProcess.setInt(3, mapRes.size)
        pstmt.executeBatch()
      }
      pstmtProcess.addBatch()
      pstmtProcess.executeBatch()

      conn.commit()
      pstmt.close()
      pstmtProcess.close()
      conn.close()

      val endtime = System.currentTimeMillis()
      println("查询消耗时间:" + (endtime - begintime) + "毫秒！")
    } else {
      if (model.equals("01")) {
        val beginTime = System.currentTimeMillis()
        val sql = getSql(map, tableName)
        val timeSql = getTimeSql(map, tableName)

        println("new自定义函数sql查询语句为:" + sql)
        println("new时间sql:" + timeSql)
        var result: Dataset[Row] = null
        if (timeSql != null && !timeSql.isEmpty()) {
          result = sqlContext.sql(sql).filter(timeSql).limit(1000)
        } else {
          //         result = sqlContext.sql(sql).limit(1000)
          result = sqlContext.sql(sql).limit(1000)
        }

        if (bInsert.equals("01")) {
          result.show(100)
        } else if (bInsert.equals("02")) {
          {

            result.foreachPartition {
              data =>
                {
                  //             val begConnect = System.currentTimeMillis()
                  //            val conn = MySQLConnectManager.getConnet(zkHostport)
                  //            conn.setAutoCommit(false)
                  //      //      val sql = "insert into xscpcb_progress(jobid,pronum,total) values(?,?,?)";
                  //            val sql = "insert into xscpcb_result(j_id, s_id, y_id,count) values(?, ?, ?, ?)"
                  //            val pstmt = conn.prepareStatement(sql);
                  //            val endConnect = System.currentTimeMillis()
                  //            println("获取连接消耗时间:"+(endConnect-begConnect)+"毫秒")
                  data.foreach { t =>
                    {
                      println("Record:" + t(1).toString())
                      //                pstmt.setString(1, job_id);
                      //                pstmt.setString(2, t(1).toString());
                      //                 pstmt.setString(3, t(2)+"");
                      //                pstmt.setString(4, t(3).toString());
                      //                pstmt.addBatch();
                    }
                  }
                  //            var test = pstmt.executeBatch()
                  //            val end2Connect = System.currentTimeMillis()
                  //            println("入partition数据结束:"+(end2Connect - endConnect)+"毫秒")
                  //            conn.commit()
                  //            pstmt.close()
                  //            conn.close()
                  //            val end3Connect = System.currentTimeMillis()
                  //            println("最后提交消耗:"+(end3Connect-end2Connect)+"毫秒")
                }
            }
          }
        } else if (bInsert.equals("03")) {
          var map: Map[String, Int] = Map()

          val conn = MySQLConnectManager.getConnet(zkHostport)
          conn.setAutoCommit(false)
          val sql = "insert into xscpcb_result(j_id, s_id, y_id,count) values(?, ?, ?, ?)"
          val pstmt = conn.prepareStatement(sql);
          result.collect().foreach { t =>
            //有的就不再入库了
            if (map.contains(t(0).toString())) {

            } else {
              map.put(t(0).toString(), 1)
              pstmt.setString(1, job_id);
              pstmt.setString(2, t(1).toString());
              pstmt.setString(3, t(2) + "");
              pstmt.setString(4, t(3).toString());
              pstmt.addBatch();
            }
          }

          pstmt.executeBatch()
          conn.commit()
          pstmt.close()
          conn.close()
        }

        val endTime = System.currentTimeMillis()
        println("执行自定义函数sql语句完成,消耗时间:" + (endTime - beginTime) + "毫秒!")
      } else if (model.equals("02")) {

        val begTime2 = System.currentTimeMillis()
        println("开始执行第二句:" + begTime2)
        val sql2 = getSql2(map, tableName)
        println("sql2:" + sql2)
        val res2 = sqlContext.sql(sql2)
        if (bInsert.equals("01")) {
          res2.show(100)
        } else if (bInsert.equals("02")) {
          {

            res2.foreachPartition { data =>
              {
                val conn = MySQLConnectManager.getConnet(zkHostport)
                conn.setAutoCommit(false)
                //      val sql = "insert into xscpcb_progress(jobid,pronum,total) values(?,?,?)";
                val sql = "insert into xscpcb_result(j_id, s_id,y_id,count) values(?, ?, ?, ?)"
                val pstmt = conn.prepareStatement(sql);

                data.foreach { t =>
                  {

                    pstmt.setString(1, job_id);
                    pstmt.setString(2, t(1).toString());
                    //          pstmt.setString(3, t(1).toString());
                    pstmt.setString(3, t(2) + "");
                    pstmt.setString(4, t(3).toString());
                    pstmt.addBatch();
                  }
                }
                var test = pstmt.executeBatch()
                conn.commit()
                pstmt.close()
                conn.close()
              }
            }

          }
        }

        val endTime2 = System.currentTimeMillis()
        println("执行第二遍完成:" + (endTime2 - begTime2) + "毫秒")
      } else if (model.equals("03")) {

        val begTime2 = System.currentTimeMillis()
        println("开始执行第三个语句:" + begTime2)
        val sql2 = getSql3(map, tableName)
        println("new sql3:" + sql2)
        val res2 = sqlContext.sql(sql2)
        if (bInsert.equals("01")) {
          res2.show(100)
        } else if (bInsert.equals("02")) {
          {

            res2.foreachPartition { rdd =>
              {
                rdd.foreach { line =>
                  println(line.toString())
                }
              }
            }

          }
        }

        val endTime2 = System.currentTimeMillis()
        println("执行第三遍完成:" + (endTime2 - begTime2) + "毫秒")

      } else if (model.equals("07")) {

        val begTime2 = System.currentTimeMillis()
        println("开始执行第7个语句:" + begTime2)

        println("sql:" + testSql)
        val res2 = sqlContext.sql(testSql)
        if (bInsert.equals("01")) {
          res2.show(100)
        } else if (bInsert.equals("02")) {

          res2.foreachPartition {
            data =>
              {
                val begConnect = System.currentTimeMillis()
                val conn = MySQLConnectManager.getConnet(zkHostport)
                conn.setAutoCommit(false)
                //      //      val sql = "insert into xscpcb_progress(jobid,pronum,total) values(?,?,?)";
                val sql = "insert into xscpcb_result(j_id, s_id, y_id,count) values(?, ?, ?, ?)"
                val pstmt = conn.prepareStatement(sql);
                val endConnect = System.currentTimeMillis()
                println("获取连接消耗时间:" + (endConnect - begConnect) + "毫秒")
                data.foreach { t =>
                  {
                    println("Record:" + t(1).toString())
                    pstmt.setString(1, job_id);
                    pstmt.setString(2, t(1).toString());
                    pstmt.setString(3, t(2) + "");
                    pstmt.setString(4, t(3).toString());
                    pstmt.addBatch();
                  }
                }
                var test = pstmt.executeBatch()
                val end2Connect = System.currentTimeMillis()
                println("入partition数据结束:" + (end2Connect - endConnect) + "毫秒")
                conn.commit()
                pstmt.close()
                conn.close()
                val end3Connect = System.currentTimeMillis()
                println("最后提交消耗:" + (end3Connect - end2Connect) + "毫秒")
              }
          }
        } else if (bInsert.equals("03")) {
          res2.foreachPartition { rdd =>
            {
              println("---------------------")
              rdd.foreach { line =>
                println(line.toString())
              }
            }
          }
        } else if (bInsert.equals("04")) {
          res2.collect().foreach { x => println(x.toString()) }
        } else if (bInsert.equals("05")) {

          var map: Map[String, Int] = Map()
          val conn = MySQLConnectManager.getConnet(zkHostport)
          conn.setAutoCommit(false)
          val sql = "insert into xscpcb_result(j_id, s_id, y_id,count) values(?, ?, ?, ?)"
          val pstmt = conn.prepareStatement(sql);
          res2.collect().foreach { t =>

            if (map.contains(t(0).toString())) {

            } else {
              map.put(t(0).toString(), 1)
              pstmt.setString(1, job_id);
              pstmt.setString(2, t(1).toString());
              pstmt.setString(3, t(2) + "");
              pstmt.setString(4, t(3).toString());
              pstmt.addBatch();
            }

          }

          pstmt.executeBatch()
          conn.commit()
          pstmt.close()
          conn.close()
        }
        val endTime2 = System.currentTimeMillis()
        println("执行第7个语句完成:" + (endTime2 - begTime2) + "毫秒")

      } else {

      }
    }

    //写数据
    //    var resultCount = ProcessCount.getInstance(sqlContext.sparkContext)
    /*
    
    res.foreachPartition { data => {
      val conn = MySQLConnectManager.getConnet(zkHostport)
      conn.setAutoCommit(false)
//      val sql = "insert into xscpcb_progress(jobid,pronum,total) values(?,?,?)";
      val sql = "insert into xscpcb_result(j_id, s_id, count, y_id) values(?, ?, ?, ?)"
      val pstmt = conn.prepareStatement(sql);

      data.foreach { t =>
        {

          pstmt.setString(1, job_id);
          pstmt.setString(2, t(1).toString());
//          pstmt.setString(3, t(1).toString());
           pstmt.setString(3, t(2)+"");
          pstmt.setString(4, t(3).toString());
          pstmt.addBatch();
        }
      }
      var test = pstmt.executeBatch()
      conn.commit()
      pstmt.close()
//      if (test != null && test.size > 0) {
//        resultCount.add(test(0))
//      }
      }
    }
    */

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

  def getSql3(param: InputBean, table: String): String = {
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

    val brand = param.carBrand
    val model = param.carModel
    val yearId = param.carYear
    val location = param.locationId
    val sql = new StringBuffer()
    //    sql.append("select platenumber,solrid,yearid,count,len FROM (select platenumber,solrid,yearid,levenshtein(platenumber,'"+plateNumber+"') as len from "+table)
    //    sql.append("select platenumber,solrid,yearid,len,capturetime,dateid FROM (select platenumber,solrid,yearid,getSimilay(platenumber,'"+plateNumber+"') as len,capturetime,dateid from "+table)
    //    sql.append("select first(platenumber) as platenumber,first(solrid) as solrid,first(yearid) as yearid,count(1) as count FROM (select platenumber,solrid,yearid,getSimilay(platenumber,'"+plateNumber+"') as len from "+table)
    sql.append("select platenumber,solrid,yearid,len FROM (select platenumber,solrid,yearid,getSimilay(platenumber,'" + plateNumber + "') as len from " + table)
    sql.append(" where platenumber != ''")

    //各种另外的条件
    if (param.carYear != null && param.carYear(0) != "" && param.carYear(0) != "0") {
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
    //    
    //    if (startTime != 0) {
    //      sql.append(" AND ltime >= ").append("unix_timestamp("+startTime+")")
    //    }
    //
    //    if (endTime != 0) {
    //      sql.append(" AND ltime <= ").append("unix_timestamp("+endTime+")")
    //    }

    if (model != null && model.length > 0 && param.carModel(0) != "0") {
      var m = model.reduce((a, b) => a + "," + b)
      sql.append(" AND modelid IN (").append(m).append(")");
    }

    if (brand != null && brand != "" && brand != "0") {
      sql.append(" AND brandid = ").append(brand)
    }

    //    sql.append(") t where t.len = "+differ)
    //    sql.append(") t where t.len = "+differ+" group by platenumber limit 100")
    sql.append(") t where t.len = " + differ + "  limit 10")
    sql.toString()

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
    val brand = param.carBrand
    val model = param.carModel
    val yearId = param.carYear
    val location = param.locationId
    val sql = new StringBuffer()
    //    sql.append("select plateNumber,solrid from "+table)
    sql.append("select platenumber,first(solrid) as solrid,first(yearid) as yearid,count(1) as count from " + table)
    sql.append(" where plateNumber != ''")

    //各种另外的条件
    if (param.carYear != null && param.carYear(0) != "" && param.carYear(0) != "0") {
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
      sql.append(" AND capturetime >= '").append(startTime).append("'")
    }

    if (endTime != 0) {
      sql.append(" AND capturetime <= '").append(endTime).append("'")
    }

    if (model != null && model.length > 0 && param.carModel(0) != "0") {
      var m = model.reduce((a, b) => a + "," + b)
      sql.append(" AND modelid IN (").append(m).append(")");
    }

    if (brand != null && brand != "" && brand != "0") {
      sql.append(" AND brandid = ").append(brand)
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
    //////////////////
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

  def getSql(param: InputBean, table: String): String = {
    val plateNumber = param.plateNumber
    val differ = param.differ
    var startTime: String = null
    if (param.startTime != null) {
      startTime = TimeUtil.getTimeStringFormat(param.startTime)
    }
    var endTime: String = null
    if (param.endTime != null) {
      endTime = TimeUtil.getTimeStringFormat(param.endTime)
    }

    val sql = new StringBuffer()
    //    sql.append("select platenumber,solrid,yearid,count,len FROM (select platenumber,solrid,yearid,levenshtein(platenumber,'"+plateNumber+"') as len from "+table)
    sql.append("select platenumber,solrid,yearid,len,capturetime,dateid FROM (select platenumber,solrid,yearid,getSimilay(platenumber,'" + plateNumber + "') as len,capturetime,dateid from " + table)
    //    sql.append("select first(platenumber,solrid ,yearid) FROM (select platenumber,solrid,yearid,levenshtein(platenumber,'"+plateNumber+"') as len from "+table)
    sql.append(" where platenumber != ''")

    //各种另外的条件
    
    if (param.locationId != null && param.locationId.length != 0 && param.locationId(0) != "" && param.locationId(0) != "0") {
      var l = param.locationId.map { "'" + _ + "'" }.reduce((a, b) => a + "," + b)
      sql.append(" AND locationid IN (").append(l).append(")")
    }
    
    
    if (param.carYear != null && param.carYear.length != 0&&param.carYear(0) != "" && param.carYear(0) != "0") {
      var m = param.carYear.reduce((a, b) => a + "," + b)
      sql.append(" AND yearid IN (").append(m).append(")");
    }

    //     if (startTime != null) {
    //      sql.append(" AND dateid >= ").append(getDateid(startTime))
    //    }
    //
    //    if (endTime != null) {
    //      sql.append(" AND dateid <= ").append(getDateid(endTime))
    //    }
    //    
    //    if (startTime != null) {
    //      sql.append(" AND capturetime >= '").append(startTime).append("'")
    //    }
    //
    //    if (endTime != null) {
    //      sql.append(" AND capturetime <= '").append(endTime).append("'")
    //    }

    if (param.carModel != null && param.carModel.length > 0 && param.carModel(0) != "0") {
      val model = param.carModel
      var m = model.reduce((a, b) => a + "," + b)
      sql.append(" AND modelid IN (").append(m).append(")");
    }

    if (param.carBrand != null && param.carBrand != "" && param.carBrand != "0") {
      val brand = param.carBrand
      sql.append(" AND brandid = ").append(brand)
    }

    sql.append(") t where t.len = " + differ)
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

  def levenshtein(s: String, t: String): Int = {
    val sLen = s.length()
    val tLen = t.length()
    var si: Int = 0
    var ti: Int = 0
    var ch1: Char = 0
    var ch2: Char = 0
    var cost: Int = 0

    if (s.equals(t)) {
      0
    } else if (s.length() == 0) {
      tLen
    } else if (t.length() == 0) {
      sLen
    } else {
      var d = Array.ofDim[Int](sLen + 1, tLen + 1)
      for (si <- 0 to sLen) {
        d(si)(0) = si
      }
      for (ti <- 0 to tLen) {
        d(0)(ti) = ti
      }
      for (si <- 1 to sLen) {
        ch1 = s.charAt(si - 1)
        for (ti <- 1 to tLen) {
          ch2 = t.charAt(ti - 1)
          if (ch1 == ch2) {
            cost = 0
          } else {
            cost = 1
          }
          d(si)(ti) = Math.min(Math.min(d(si - 1)(ti) + 1, d(si)(ti - 1) + 1), d(si - 1)(ti - 1) + cost)
        }
      }

      d(sLen)(tLen)
    }
  }
}

object ProcessCount {

  @volatile private var instance: LongAccumulator = null

  def getInstance(sc: SparkContext): LongAccumulator = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.longAccumulator("ProcessCount")
        }
      }
    }
    instance
  }
}

object Main {
  def main(args: Array[String]): Unit = {

    val test = new SparkEngineV2ForSimilarPlate
    //println(test.getTimeStampSecond("20161024105710"))
    println(test.getDateid("20161024105710"))

    //    val test = new SparkEngineV2ForSimilarPlate
    //    val x = InputBean.apply("2016", "201610", Array("1","2"), Array("3","4"), "5", Array("6","7"), "8", "鲁ABCDEF", 1, "aaa", 2)
    /*
     * startTime: String,
    endTime: String,
    locationId: Array[String],
    carModel: Array[String],
    carBrand: String,
    carYear: Array[String],
    carColor: String,
    plateNumber: String, 
    count: Int,
    feature:String,
    differ:Int
     */
    //    val gson = new Gson
    //    val r = gson.toJson(x)
    //    println(r)
    //    val sql = test.getSql2(x, "test")
    //    println(sql)
  }

}


