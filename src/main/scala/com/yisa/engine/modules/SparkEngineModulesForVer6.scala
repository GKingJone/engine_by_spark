package com.yisa.engine.modules

import com.google.gson.Gson
import java.text.SimpleDateFormat
import java.util.ArrayList
import java.util.Base64
import java.util.Calendar
import java.util.Date
import java.util.Properties

import org.apache.commons.cli._
import org.apache.kafka.clients._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import com.google.gson.reflect.TypeToken
import com.yisa.engine.branch._
import com.yisa.engine.branch.SparkEngineV2ForFrequentlyCar
import com.yisa.engine.branch.SparkEngineV2ForSearchCarByPic
import com.yisa.engine.branchV3.SparkEngineV3ForFrequentlyCarPresto
import com.yisa.engine.common.CommonTaskType
import com.yisa.engine.common.InputBean
import com.yisa.engine.db.MySQLConnectManager
import com.yisa.engine.uitl.TimeUtil
import com.yisa.wifi.zookeeper.ZookeeperUtil
import com.yisa.engine.branch.SparkEngineV2ForMultiPointPresto
import java.util.concurrent.ExecutorService
import com.yisa.engine.branchV3.SparkEngineV3ForSearchCarByPicPresto
import com.yisa.engine.branchV5.SparkEngineV5ForSearchCarByPic
import com.yisa.engine.branchV5.SparkEngineV5ForFrequentlyCar_HBase
import com.yisa.engine.branchV5.SparkEngineV5ForSearchCarByPic_Redis
import com.yisa.engine.branchV5.SparkEngineV5ForSearchCarByPic_Redis
import com.yisa.engine.branchV5.SparkEngineV5ForSearchCarByPic_HBase
import com.yisa.engine.branchV5.SparkEngineV5ForTravelTogether_HBase
import com.yisa.engine.branchV5.SparkEngineV5ForMultiPoint_HBase
import com.yisa.engine.branchV5.SparkEngineV5ForMultiPoint_HBase2
import com.yisa.engine.branchV5.SparkEngineV5ForNocturnal
import com.yisa.engine.branchV5.SparkEngineV5ForNocturnalPresto
import com.yisa.engine.branchV5.SparkEngineV5ForNocturnalPresto
import scala.collection.mutable.ArrayBuffer

object SparkEngineModulesForVer6 {

  def FrequentlyCar(line: String, sparkSession: SparkSession, tableName: String, threadPool: ExecutorService, prestoTableName: String, frequentlyCarResultTableName: String, zkHostPort: String, cacheDays: Int, prestoHostPort: String, allLocations: ArrayBuffer[String]) {

    threadPool.execute(new SparkEngineV3ForFrequentlyCarPresto(line, prestoTableName, frequentlyCarResultTableName, zkHostPort, prestoHostPort))

  }

  import scala.collection.mutable.Set
  import scala.collection.mutable.Map
  def SearchCarByPic(line: String, sparkSession: SparkSession, tableName: String, zkHostPort: String, cacheDays4SearchCar: Int, threadPool: ExecutorService, prestoHostPort: String, prestoTableName: String, allYearidByBrandid: Map[Int, Set[Long]], allYearidByModelid: Map[Int, Set[Long]], AllYearids: scala.collection.mutable.ArrayBuffer[String], useRedisOrNot: Boolean) {

    var line_arr = line.split("\\|")

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val map: InputBean = gson.fromJson[InputBean](line_arr(2), mapType)

    val startTime = map.startTime
    val startTimeDateid = TimeUtil.getDateId(map.startTime)

    if (startTimeDateid < getLCacheDataDateid(cacheDays4SearchCar).toInt) {
      println("startTimeDateid:" + startTimeDateid)
      println("getLCacheDataDateid(cacheDays4SearchCar).toInt:" + getLCacheDataDateid(cacheDays4SearchCar).toInt)
      println("all time data!!")
      //      threadPool.execute(new SparkEngineV3ForSearchCarByPicPresto(sparkSession, line, prestoTableName, zkHostPort, prestoHostPort))
      val fcarHBase = new SparkEngineV5ForSearchCarByPic_HBase()
      fcarHBase.SearchCarByPicParallelized(sparkSession, line, tableName, zkHostPort, allYearidByBrandid, allYearidByModelid, AllYearids)

    } else {

      if (useRedisOrNot) {
        SearchCarByPic_Redis(line, sparkSession, tableName, zkHostPort, cacheDays4SearchCar, threadPool, prestoHostPort, prestoTableName, allYearidByBrandid, allYearidByModelid, AllYearids)
      } else {
        SearchCarByPic_HBase(line, sparkSession, tableName, zkHostPort, cacheDays4SearchCar, threadPool, prestoHostPort, prestoTableName, allYearidByBrandid, allYearidByModelid, AllYearids)
      }
    }
  }

  private def SearchCarByPic_HBase(line: String, sparkSession: SparkSession, tableName: String, zkHostPort: String, cacheDays: Int, threadPool: ExecutorService, prestoHostPort: String, prestoTableName: String, allYearidByBrandid: Map[Int, Set[Long]], allYearidByModelid: Map[Int, Set[Long]], AllYearids: scala.collection.mutable.ArrayBuffer[String]) {
    var line_arr = line.split("\\|")

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val map: InputBean = gson.fromJson[InputBean](line_arr(2), mapType)

    val startTime = map.startTime
    val startTimeDateid = TimeUtil.getDateId(map.startTime)

    val fcarhbase = new SparkEngineV5ForSearchCarByPic_HBase()
    println("cache time data!!")
    val endTimeDateid = TimeUtil.getDateId(map.endTime)
    //    threadPool.execute(new SparkEngineV3ForSearchCarByPicPresto(sparkSession, line, prestoTableName, zkHostPort, prestoHostPort))
    fcarhbase.SearchCarByPicParallelized(sparkSession, line, tableName, zkHostPort, allYearidByBrandid, allYearidByModelid, AllYearids)

  }

  private def SearchCarByPic_Redis(line: String, sparkSession: SparkSession, tableName: String, zkHostPort: String, cacheDays: Int, threadPool: ExecutorService, prestoHostPort: String, prestoTableName: String, allYearidByBrandid: Map[Int, Set[Long]], allYearidByModelid: Map[Int, Set[Long]], AllYearids: scala.collection.mutable.ArrayBuffer[String]) {
    var line_arr = line.split("\\|")

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val map: InputBean = gson.fromJson[InputBean](line_arr(2), mapType)

    val startTime = map.startTime
    val startTimeDateid = TimeUtil.getDateId(map.startTime)

    if (startTimeDateid <= getLCacheDataDateid(cacheDays).toInt) {
      println("all time data!!")
      val fcarHBase = new SparkEngineV5ForSearchCarByPic_HBase()
      //      threadPool.execute(new SparkEngineV3ForSearchCarByPicPresto(sparkSession, line, prestoTableName, zkHostPort, prestoHostPort))
      fcarHBase.SearchCarByPicParallelized(sparkSession, line, tableName, zkHostPort, allYearidByBrandid, allYearidByModelid, AllYearids)

    } else {
      val fcarRedis = new SparkEngineV5ForSearchCarByPic_Redis()
      println("cache time data!!")
      val endTimeDateid = TimeUtil.getDateId(map.endTime)

      //    threadPool.execute(new SparkEngineV3ForSearchCarByPicPresto(sparkSession, line, prestoTableName, zkHostPort, prestoHostPort))
      if (endTimeDateid - startTimeDateid > 4) {
        fcarRedis.SearchCarByPicParallelized(sparkSession, line, tableName, zkHostPort, allYearidByBrandid, allYearidByModelid, AllYearids)
      } else {
        fcarRedis.SearchCarByPic(sparkSession, line, tableName, zkHostPort, allYearidByBrandid, allYearidByModelid, AllYearids)
      }
    }
  }

  def SearchSimilarPlate(sparkData: Dataset[Row], line: String, sparkSession: SparkSession, threadPool: ExecutorService, tableName: String, prestoTableName: String, zkHostPort: String, cacheDays: Int, prestoHostPort: String) {

    threadPool.execute(new SparkEngineV2ForSimilarPlatePresto(line, prestoTableName, zkHostPort, prestoHostPort))

  }

  def MultiPoint(sparkData: Dataset[Row], line: String, sparkSession: SparkSession, threadPool: ExecutorService, tableName: String, prestoTableName: String, zkHostPort: String, cacheDays: Int, prestoHostPort: String) {

    var line_arr = line.split("\\|")
    val gson = new Gson
    val mapType = new TypeToken[Array[InputBean]] {}.getType
    val maps: Array[InputBean] = gson.fromJson[Array[InputBean]](line_arr(2), mapType)

    var allTime = false
    maps.foreach { map =>
      {

        val startTime = map.startTime
        val startTimeDateid = TimeUtil.getDateId(map.startTime)
        if (startTimeDateid <= getLCacheDataDateid(cacheDays).toInt)
          allTime = true
      }
    }

    if (allTime) {
      println("all time data!!")
      threadPool.execute(new SparkEngineV2ForMultiPointPresto(line, prestoTableName, zkHostPort, prestoHostPort))
    } else {
      println("cache time data!!")
      //      val mp = new SparkEngineV2ForMultiPoint()
      //      mp.searchMultiPoint(sparkData, sparkSession, line, tableName, zkHostPort)
      val mp = new SparkEngineV5ForMultiPoint_HBase2()
      mp.MultiPointParallelized(sparkSession, line, tableName, zkHostPort)
    }

  }

  def CaseCar(sparkData: Dataset[Row], line: String, sparkSession: SparkSession, threadPool: ExecutorService, tableName: String, prestoTableName: String, zkHostPort: String, cacheDays: Int, prestoHostPort: String) {

    threadPool.execute(new SparkEngineV2ForCaseCarPresto(line, prestoTableName, zkHostPort, prestoHostPort))

  }

  def EndStation(sparkData: Dataset[Row], line: String, sparkSession: SparkSession, threadPool: ExecutorService, tableName: String, prestoTableName: String, zkHostPort: String, cacheDays: Int, prestoHostPort: String) {
    threadPool.execute(new SparkEngineV2ForEndStationPresto(line, prestoTableName, zkHostPort, prestoHostPort))
  }

  def TogetherCar(sparkSession: SparkSession, line: String, tableName: String, zkHostPort: String, url: String) {
    val tc2 = new SparkEngineV5ForTravelTogether_HBase()
    tc2.TravelTogether(sparkSession, line, tableName, zkHostPort, url)
  }

  def ExportLocation(sparkData: Dataset[Row], sparkSession: SparkSession, line: String, tableName: String, zkHostPort: String) {

    val tc = new SparkEngineV2ForExportLocation()
    tc.searchExportLocation(sparkData, sparkSession, line, tableName, zkHostPort)

  }

  /**
   * 昼伏夜出
   */
  def ConcealOneselfByDayAndMarchByNight(line: String, sparkSession: SparkSession, tableName: String, zkHostPort: String, cacheDays: Int, threadPool: ExecutorService, prestoHostPort: String, prestoTableName: String) {
    var line_arr = line.split("\\|")

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val map: InputBean = gson.fromJson[InputBean](line_arr(2), mapType)

    var dateStr = map.carColor.split(",")
    val startTime = dateStr(0)
    //  val startTimeDateid = TimeUtil.getDateId(map.startTime)

    //    val fcar = new SparkEngineV2ForFrequentlyCar()

    val nocaturnal = new SparkEngineV5ForNocturnal()

    if (startTime.toInt <= getLCacheDataDateid(cacheDays).toInt) {
      println("all time data!!")
      //开线程，走presoto

      threadPool.execute(new SparkEngineV5ForNocturnalPresto(line, prestoTableName, tableName, zkHostPort, prestoHostPort))
      //      threadPool.execute(new SparkEngineV3ForFrequentlyCarPresto(line, prestoTableName, frequentlyCarResultTableName, zkHostPort, prestoHostPort))
    } else {
      println("cache time data!!")
      //调用spark alluxio数据

      nocaturnal.Nocturnal(sparkSession, line, tableName, tableName, zkHostPort)
      //      fcar.FrequentlyCar(sparkSession, line, tableName, frequentlyCarResultTableName, zkHostPort)

    }
  }

  //决定系统缓存多少数据，以天为单位
  def getLCacheDataDateid(days: Int): String = {
    var cal = Calendar.getInstance();
    cal.setTime(new Date());
    cal.add(Calendar.DAY_OF_MONTH, -days);
    val format = new SimpleDateFormat("yyyyMMdd")
    format.format(cal.getTime())
  }
}