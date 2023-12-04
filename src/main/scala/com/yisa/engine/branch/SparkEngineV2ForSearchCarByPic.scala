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
import com.yisa.engine.db.JDBCConnectionSinglen
import redis.clients.jedis.Jedis
import com.yisa.engine.db.RedisConnectionPool

/**
 * @author liliwei
 * @date  2016年9月9日
 * 以图搜车
 */
class SparkEngineV2ForSearchCarByPic {

  def SearchCarByPic(sqlContext: SparkSession, line: String, tableName: String, zkHostport: String): Unit = {
    val now1 = new Date().getTime()
    var line_arr = line.split("\\|")
    val job_id = line_arr(1)
    val line2 = line_arr(2).split(",")

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val map: InputBean = gson.fromJson[InputBean](line_arr(2), mapType)

    //得到执行SQL
    val sql1 = getSQL(map, tableName, true)

    println("SQL--------------:" + sql1)
    //    val resultData = executeSparkSQLUsePartition(sqlContext, sql1, zkHostport, job_id)
    //    val resultData = executeSparkSQLUsePartition(sqlContext, sql1, zkHostport, job_id)

    val resultData2 = executeSparkSQLNoPartition(sqlContext, sql1, zkHostport, job_id)
    val resultData = executeSparkSQLNoPartitionInsertRedis(sqlContext, sql1, zkHostport, job_id)
    //    val resultData3 = executeSparkSQLNoPartitionNotInSertDatabase(sqlContext, sql1, zkHostport, job_id)
    //    val resultData4 = executeSparkSQLUsePartitionNotInSertDatabase(sqlContext, sql1, zkHostport, job_id)

    val insertMySQLEnd = new Date().getTime
    //    val tempCount = resultData.count()
    val tempCount = resultData2

    if (tempCount == 0) {
      println("have no result first time ,set similarityLimit false ,run again , tempCount :" + tempCount)
      val sql2 = getSQL(map, tableName, false)
      println("SQL--------------:" + sql2)
      executeSparkSQLNoPartition(sqlContext, sql2, zkHostport, job_id)
    }
    val now3 = new Date().getTime
    println("have  result or not , time :" + (now3 - insertMySQLEnd))
  }

  def executeSparkSQLUsePartition(sqlContext: SparkSession, sql_S: String, zkHostport: String, job_id: String): DataFrame = {
    val now1 = new Date().getTime
    //run spark task start-------
    val resultData = sqlContext.sql(sql_S)
    val sparkTaskEnd = new Date().getTime
    //run spark task end-------
    //    println("get result time :" + (sparkTaskEnd - now1))

    //insert data into mysql start--------
    // the connections in the pool should be lazily created on demand and timed out if not used for a while. 

    resultData.foreachPartition { data =>

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

    }
    //insert data into mysql end--------
    val insertMySQLEnd = new Date().getTime
    println("executeSparkSQLUsePartition  time :" + (insertMySQLEnd - sparkTaskEnd))
    return resultData
  }

  def executeSparkSQLUsePartitionNotInSertDatabase(sqlContext: SparkSession, sql_S: String, zkHostport: String, job_id: String): DataFrame = {
    val now1 = new Date().getTime
    //run spark task start-------
    val resultData = sqlContext.sql(sql_S)
    //run spark task end-------
    //    println("get result time :" + (sparkTaskEnd - now1))

    //insert data into mysql start--------
    // the connections in the pool should be lazily created on demand and timed out if not used for a while. 

    resultData.foreachPartition { data =>

      data.foreach { t =>
        {
          println(t)

        }
      }

    }
    //insert data into mysql end--------
    val insertMySQLEnd = new Date().getTime
    println("executeSparkSQLUsePartitionNotInSertDatabase  time :" + (insertMySQLEnd - now1))
    return resultData
  }

  def executeSparkSQLNoPartition(sqlContext: SparkSession, sql_S: String, zkHostport: String, job_id: String): Int = {
    //run spark task start-------
    val now1 = new Date().getTime
    val resultData = sqlContext.sql(sql_S)
    val sparkTaskEnd = new Date().getTime
    //run spark task end-------
    println("get result time :" + (sparkTaskEnd - now1))

    //insert data into mysql start--------
    // the connections in the pool should be lazily created on demand and timed out if not used for a while. 

    val preMysqlPrepare = new Date().getTime
    val conn = MySQLConnectManager.getConnet(zkHostport)
    conn.setAutoCommit(false)
    val sql = "insert into gpu_index_job (job_id,solr_id,sort_value,plate_number) values(?,?,?,?)";
    val pstmt = conn.prepareStatement(sql);
    val postMysqlPrepare = new Date().getTime

    println("postMysqlPrepare  time :" + (postMysqlPrepare - preMysqlPrepare))

    //get result data
    val resultData2 = resultData.collect()
    val getCollect = new Date().getTime
    println("getCollect  time :" + (getCollect - postMysqlPrepare))
    //have result or not 
    var tempCount = 0
    resultData2.foreach { t =>
      {
        tempCount += 1
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
    val insertMySQLEnd = new Date().getTime
    println("executeSparkSQLNoPartition  time :" + (insertMySQLEnd - sparkTaskEnd))

    return tempCount
  }

  def executeSparkSQLNoPartitionInsertRedis(sqlContext: SparkSession, sql_S: String, zkHostport: String, job_id: String): Int = {
    //run spark task start-------
    val now1 = new Date().getTime
    val resultData = sqlContext.sql(sql_S)
    val sparkTaskEnd = new Date().getTime

    val jedis: Jedis = RedisConnectionPool.getJedis(zkHostport);
    val resultData2 = resultData.collect()
    //have result or not    
    var tempCount = 0
    resultData2.foreach { t =>
      {

        //job_id,solr_id,sort_value,plate_number
        // pstmt.setString(1, job_id);
        //        pstmt.setString(2, t(0).toString());
        //        pstmt.setString(3, t(1).toString());
        //        pstmt.setString(4, t(2).toString());

        RedisConnectionPool.set(8, jedis, job_id + "_" + tempCount, t(0).toString() + ":" + t(1).toString() + ":" + t(2).toString(), 1)
        tempCount += 1
      }
    }
    RedisConnectionPool.returnResource(jedis)

    val insertMySQLEnd = new Date().getTime
    println("executeSparkSQLNoPartitionInsertRedis  time :" + (insertMySQLEnd - sparkTaskEnd))

    return tempCount
  }

  def executeSparkSQLNoPartitionNotInSertDatabase(sqlContext: SparkSession, sql_S: String, zkHostport: String, job_id: String): Int = {
    //run spark task start-------
    val now1 = new Date().getTime
    val resultData = sqlContext.sql(sql_S)
    val sparkTaskEnd = new Date().getTime
    //run spark task end-------
    println("get result time :" + (sparkTaskEnd - now1))

    //insert data into mysql start--------
    // the connections in the pool should be lazily created on demand and timed out if not used for a while. 

    //get result data
    val resultData2 = resultData.collect()
    //have result or not 
    var tempCount = 0
    resultData2.foreach { t =>
      {

        tempCount += 1
        println(t)

      }
    }

    val insertMySQLEnd = new Date().getTime
    println("executeSparkSQLNoPartitionNotInSertDatabase time :" + (insertMySQLEnd - sparkTaskEnd))

    return tempCount
  }

  def getSQL(map: InputBean, tableName: String, similarityLimit: Boolean) = {
    val feature = map.feature

    val carBrand = map.carBrand

    val carModel = map.carModel

    //    val startTime = TimeUtil.getTimeStringFormat(map.startTime)
    //    val endTime = TimeUtil.getTimeStringFormat(map.endTime)
    val startTime = TimeUtil.getTimestampLong(map.startTime)
    val endTime = TimeUtil.getTimestampLong(map.endTime)

    val startTimeDateid = TimeUtil.getDateId(map.startTime)
    val endTimeDateid = TimeUtil.getDateId(map.endTime)

    val sb = new StringBuffer();

    sb.append(" SELECT  solrid,similarity ,plateNumber FROM ")
    sb.append(" (")
    sb.append(" SELECT solrid,getSimilarity('" + feature + "',recFeature) as similarity,plateNumber as plateNumber ")

    sb.append(" FROM " + tableName + " where   recFeature != '' and recFeature != 'null' ")

    if (map.carYear != null && map.carYear(0) != "" && map.carYear(0) != "0") {
      var m = map.carYear.reduce((a, b) => a + "," + b)
      sb.append(" AND yearid IN (").append(m).append(")");
    }

    if (startTime != 0L) {
      sb.append(" AND capturetime >= ").append(startTime)
    }

    if (endTime != 0L) {
      sb.append(" AND capturetime <= ").append(endTime)
    }

    if (startTime != 0L) {
      sb.append(" AND dateid >= ").append(startTimeDateid)
    }

    if (endTime != 0L) {
      sb.append(" AND dateid <= ").append(endTimeDateid)
    }

    //    if (startTime != null) {
    //      sb.append(" AND capturetime >= '").append(startTime).append("'")
    //    }
    //
    //    if (endTime != null) {
    //      sb.append(" AND capturetime <= '").append(endTime).append("'")
    //    }

    if (carModel != null && carModel.length > 0 && map.carModel(0) != "0") {
      var m = carModel.reduce((a, b) => a + "," + b)
      sb.append(" AND modelid IN (").append(m).append(")");
    }

    if (carBrand != null && carBrand != "" && carBrand != "0") {
      sb.append(" AND brandid = ").append(carBrand)
//      sb.append(" AND bp = ").append(carBrand.toInt % 20)
    }

    //    sb.append("  and modelId  = " + modelId)
    sb.append(" ) t  ")

    if (similarityLimit) {
      sb.append(" where ")
      sb.append(" t.similarity<= 20000")
    }

    sb.append(" order by similarity limit 100  ")
    sb.toString()
  }
  def SearchCarByPic2(sqlContext: SparkSession, line: String, tableName: String, zkHostport: String): Unit = {
    val now1 = new Date().getTime()
    var line_arr = line.split("\\|")
    val job_id = line_arr(1)
    val line2 = line_arr(2).split(",")

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val map: InputBean = gson.fromJson[InputBean](line_arr(2), mapType)

    //得到执行SQL
    val sql1 = getSQL(map, tableName, true)

    //run spark task  and insert jdbc start-------
    var tempCount = executeSparkSQLNoPartition(sqlContext, sql1, zkHostport, job_id)
    //run spark task  and insert jdbc end-------
    val insertMySQLEnd = new Date().getTime
    if (tempCount == 0) {
      println("have no result first time ,set similarityLimit false ,run again , tempCount :" + tempCount)
      val sql2 = getSQL(map, tableName, false)
      executeSparkSQLUsePartition(sqlContext, sql2, zkHostport, job_id)
    }
    //judge result 
    val secondTime = new Date().getTime
    println("have  result or not , time :" + (secondTime - insertMySQLEnd))
  }
  def getvalue(text: String, test2: String): Long = {
    val byteData = Base64.getDecoder.decode(text)
    val oldData = Base64.getDecoder.decode(test2)
    var num: Long = 0
    for (i <- 0 until byteData.length if test2.length > 30) {
      var n = (byteData(i) & 0xff) - (oldData(i) & 0xff)
      num += n * n;
    }
    num

  }
}
 




  





