package com.yisa.engine.branch

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.util.Base64
import org.apache.kafka.clients._
import org.apache.spark.sql.SparkSession
import com.yisa.engine.db.MySQLConnectManager
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.yisa.engine.common.InputBean
import com.yisa.engine.uitl.TimeUtil
import org.apache.spark.util.LongAccumulator
import org.apache.spark.SparkContext
import java.util.Date

/**
 * @author liliwei
 * @date  2016年9月9日
 * Spark 分支任务实例
 */
class SparkEngineV2ForExample {

  def SearchCarByPic(sparkData: Dataset[Row], sparkSession: SparkSession, line: String, tableName: String, zkHostport: String): Unit = {
    val now1 = new Date().getTime()
    val line_arr = line.split("\\|")
    val job_type = line_arr(0)
    val job_id = line_arr(1)

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val map: InputBean = gson.fromJson[InputBean](line_arr(2), mapType)

    //get SQL
    val sql1 = getSQL(map, tableName, true)
    println("SQL--------------:" + sql1)

    //execute SQL
    val resultData = executeSparkSQLUsePartition(sparkSession, sql1, zkHostport, job_id)
    val insertMySQLEnd = new Date().getTime

    //get result count 
    val tempCount = resultData.count()
    if (tempCount == 0) {
      println("have no result first time ,set similarityLimit false ,run again , tempCount :" + tempCount)
      val sql2 = getSQL(map, tableName, false)
      println("SQL--------------:" + sql2)
      executeSparkSQLUsePartition(sparkSession, sql2, zkHostport, job_id)
    }
    val now3 = new Date().getTime
    println("have  result or not , time :" + (now3 - insertMySQLEnd))
    //    println("countTest" + resultCount.value)
  }

  def executeSparkSQLUsePartition(sqlContext: SparkSession, sql_S: String, zkHostport: String, job_id: String): DataFrame = {
    val now1 = new Date().getTime
    //run spark task start-------
    val resultData = sqlContext.sql(sql_S)
    val sparkTaskEnd = new Date().getTime
    //run spark task end-------
    println("get result time :" + (sparkTaskEnd - now1))

    //insert data into mysql start--------
    // the connections in the pool should be lazily created on demand and timed out if not used for a while. 
    resultData.foreachPartition { data =>

      // val conn = MySQLConnectionPool.getConnet(zkHostport)
      val conn = MySQLConnectManager.getConnet(zkHostport)
      conn.setAutoCommit(false)
      val sql = "insert into gpu_index_job (job_id,solr_id,sort_value,plate_number) values(?,?,?,?)";
      val pstmt = conn.prepareStatement(sql);

      data.foreach { t =>
        {

          pstmt.setString(1, job_id);
          pstmt.setString(2, t(0).toString());
          pstmt.setString(3, t(1).toString());
          pstmt.setString(4, t(2).toString());
          pstmt.addBatch();
        }
      }
      var test = pstmt.executeBatch()
      conn.commit()
      pstmt.close()
        conn.close()
      //      MySQLConnectionPool.releaseConn(conn)
      //      if (test != null && test.size > 0) {
      //        resultCount.add(test(0))
      //      }
    }
    //insert data into mysql end--------
    val insertMySQLEnd = new Date().getTime
    println("insert mysql  time :" + (insertMySQLEnd - sparkTaskEnd))
    return resultData
  }

  def getSQL(map: InputBean, tableName: String, similarityLimit: Boolean) = {
    val feature = map.feature

    val carBrand = map.carBrand

    val carModel = map.carModel

    val startTime = TimeUtil.getTimeStringFormat(map.startTime)
    val endTime = TimeUtil.getTimeStringFormat(map.endTime)

    val sb = new StringBuffer();

    sb.append(" SELECT  solrid,similarity ,plateNumber FROM ")
    sb.append(" (")
    sb.append(" SELECT solrid,getSimilarity('" + feature + "',recFeature) as similarity,plateNumber as plateNumber ")

    sb.append(" FROM " + tableName + " where  recFeature != '' and recFeature != 'null' ")

    if (map.carYear != null && map.carYear(0) != "" && map.carYear(0) != "0") {
      var m = map.carYear.reduce((a, b) => a + "," + b)
      sb.append(" AND yearid IN (").append(m).append(")");
    }

    if (startTime != null) {
      sb.append(" AND capturetime >= '").append(startTime).append("'")
    }

    if (endTime != null) {
      sb.append(" AND capturetime <= '").append(endTime).append("'")
    }

    if (carModel != null && carModel.length > 0 && map.carModel(0) != "0") {
      var m = carModel.reduce((a, b) => a + "," + b)
      sb.append(" AND modelid IN (").append(m).append(")");
    }

    if (carBrand != null && carBrand != "" && carBrand != "0") {
      sb.append(" AND brandid = ").append(carBrand)
    }

    //    sb.append("  and modelId  = " + modelId)
    sb.append(" ) t  ")

    if (similarityLimit) {
      sb.append(" where ")
      sb.append(" t.similarity< 20000")
    }

    sb.append(" order by similarity limit 100  ")
    sb.toString()
  }

}
