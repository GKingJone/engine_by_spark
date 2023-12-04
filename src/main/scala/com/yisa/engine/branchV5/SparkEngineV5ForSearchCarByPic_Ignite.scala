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
import com.yisa.engine.db.RedisClient
import com.yisa.engine.db.JDBCConnectionSinglen
import redis.clients.jedis.Jedis
import java.util.ArrayList
import scala.collection.immutable.TreeMap
import com.yisa.engine.db.RedisClusterClientZK
import scala.collection.mutable.ArrayBuffer
import com.yisa.engine.db.RedisClusterClientZK
import com.yisa.engine.accumulator.SimilarityAccumulatorV2

/**
 * @author liliwei
 * @date  2017年2月27日
 * 以图搜车
 */
class SparkEngineV2ForSearchCarByPic_Ignite extends Serializable {

  def SearchCarByPic(sqlContext: SparkSession, line: String, tableName: String, zkHostport: String): Unit = {

    val now1 = new Date().getTime()
    var line_arr = line.split("\\|")
    val job_id = line_arr(1)
    val line2 = line_arr(2).split(",")

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val map: InputBean = gson.fromJson[InputBean](line_arr(2), mapType)

    val startTime = TimeUtil.getTimestampLong(map.startTime)
    val endTime = TimeUtil.getTimestampLong(map.endTime)

    val startTimeDateid = TimeUtil.getDateId(map.startTime)
    val endTimeDateid = TimeUtil.getDateId(map.endTime)

    var yearidQ = ""

    var treeMap: TreeMap[Long, String] = new TreeMap[Long, String]()

    val jedis = RedisClusterClientZK.getJedisCluster(zkHostport)

    if (map.carYear != null && map.carYear(0) != "" && map.carYear(0) != "0") {
      var m = map.carYear.foreach { yearid =>
        {
          for (dateid <- startTimeDateid to endTimeDateid) {
            val values = jedis.lrange(dateid + "_" + yearid, 0, -1)
            for (index <- 0 to values.size - 1) {
              var value = values.get(index)
              var recfeature = value.split("_")(0)
              var platenumber = value.split("_")(1)
              var solrid = value.split("_")(2)

              val similarity = getSimilarity(map.feature, recfeature)
              if (similarity < 50000) {
                treeMap = treeMap.+(similarity -> (platenumber + "_" + solrid))
              }
            }
          }
        }
      }
    }

    if (treeMap.size <= 0) {
      if (map.carYear != null && map.carYear(0) != "" && map.carYear(0) != "0") {
        var m = map.carYear.foreach { yearid =>
          {
            for (dateid <- startTimeDateid to endTimeDateid) {
              val values = jedis.lrange(dateid + "_" + yearid, 0, -1)
              for (index <- 0 to values.size - 1) {
                var value = values.get(index)
                var recfeature = value.split("_")(0)
                var platenumber = value.split("_")(1)
                var solrid = value.split("_")(2)

                val similarity = getSimilarity(map.feature, recfeature)
                treeMap = treeMap.+(similarity -> (platenumber + "_" + solrid))

              }
            }

          }
        }
      }
    }

    val conn = MySQLConnectManager.getConnet(zkHostport)
    conn.setAutoCommit(false)
    val sql = "insert into gpu_index_job (job_id,solr_id,sort_value,plate_number) values(?,?,?,?)";
    val pstmt = conn.prepareStatement(sql);
    var tempCount = 0
    import scala.util.control.Breaks._
    breakable {
      treeMap.foreach(m => {

        pstmt.setString(1, job_id);
        pstmt.setString(2, m._2.split("_")(1));
        pstmt.setString(3, m._1.toString());
        pstmt.setString(4, m._2.split("_")(0));
        pstmt.addBatch();
        if (tempCount >= 100)
          break;
        tempCount += 1
      })
    }

    var test = pstmt.executeBatch()
    conn.commit()
    pstmt.close()
    conn.close()

  }

  def SearchCarByPicParallelized(sparkSession: SparkSession, line: String, tableName: String, zkHostport: String): Unit = {

    val now1 = new Date().getTime()
    var line_arr = line.split("\\|")
    val job_id = line_arr(1)
    val line2 = line_arr(2).split(",")

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val map: InputBean = gson.fromJson[InputBean](line_arr(2), mapType)

    val startTime = TimeUtil.getTimestampLong(map.startTime)
    val endTime = TimeUtil.getTimestampLong(map.endTime)

    val startTimeDateid = TimeUtil.getDateId(map.startTime)
    val endTimeDateid = TimeUtil.getDateId(map.endTime)

    var yearidQ = ""

    val accumulator = new SimilarityAccumulatorV2()
    sparkSession.sparkContext.register(accumulator, "SimilarityAccumulator")
    accumulator.reset()

    val indexs = ArrayBuffer[String]()
    for (dateid <- startTimeDateid to endTimeDateid) {
      if (map.carYear != null && map.carYear(0) != "" && map.carYear(0) != "0") {
        var m = map.carYear.foreach { yearid =>
          indexs += (dateid + "_" + yearid)
        }
      }
    }
    val dataIndexes = sparkSession.sparkContext.parallelize(indexs)
    dataIndexes.foreach { index =>
      {
        val jedis = RedisClusterClientZK.getJedisCluster(zkHostport)
        val values = jedis.lrange(index, 0, -1)
        for (index <- 0 to values.size - 1) {
          var value = values.get(index)
          var recfeature = value.split("_")(0)
          var platenumber = value.split("_")(1)
          var solrid = value.split("_")(2)
          val similarity = getSimilarity(map.feature, recfeature)
          if (similarity < 50000) {
            accumulator.add(similarity + "_" + platenumber + "_" + solrid)
          }
        }
      }
    }

    val conn = MySQLConnectManager.getConnet(zkHostport)
    conn.setAutoCommit(false)
    val sql = "insert into gpu_index_job (job_id,solr_id,sort_value,plate_number) values(?,?,?,?)";
    val pstmt = conn.prepareStatement(sql);
    var tempCount = 0
    var treeMap: TreeMap[Long, String] = accumulator.value
    import scala.util.control.Breaks._
    breakable {
      treeMap.foreach(m => {
        pstmt.setString(1, job_id);
        pstmt.setString(2, m._2.split("_")(1));
        pstmt.setString(3, m._1.toString());
        pstmt.setString(4, m._2.split("_")(0));
        pstmt.addBatch();
        if (tempCount >= 100)
          break;
        tempCount += 1
      })
    }

    println("第一次取出数据：" + tempCount + "条")
    if (tempCount == 0) {

      dataIndexes.foreach { index =>
        {
          val jedis = RedisClusterClientZK.getJedisCluster(zkHostport)
          val values = jedis.lrange(index, 0, -1)
          for (index <- 0 to values.size - 1) {
            var value = values.get(index)
            var recfeature = value.split("_")(0)
            var platenumber = value.split("_")(1)
            var solrid = value.split("_")(2)
            val similarity = getSimilarity(map.feature, recfeature)
            accumulator.add(similarity + "_" + platenumber + "_" + solrid)
          }
        }
      }

      treeMap = accumulator.value
      breakable {
        treeMap.foreach(m => {
          pstmt.setString(1, job_id);
          pstmt.setString(2, m._2.split("_")(1));
          pstmt.setString(3, m._1.toString());
          pstmt.setString(4, m._2.split("_")(0));
          pstmt.addBatch();
          if (tempCount >= 100)
            break;
          tempCount += 1
        })
      }
    }

    var test = pstmt.executeBatch()
    conn.commit()
    pstmt.close()
    conn.close()
  }

  def getSimilarity(text: String, test2: String): Long = {
    val byteData = Base64.getDecoder.decode(text)
    val oldData = Base64.getDecoder.decode(test2)
    var num: Long = 0
    for (i <- 0 until byteData.length if test2.length > 30) {
      var n = (byteData(i) & 0xff) - (oldData(i) & 0xff)
      num += n * n;
      if (num > 50000l) {
        return num
      }

    }
    return num
  }

}