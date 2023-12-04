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
import akka.actor.ActorSystem
import akka.actor.Props
import com.yisa.engine.branchV5.SparkEngineV5ForSearchCarByPic

object SparkEngineModulesForVer5 {
  
  
  

  def FrequentlyCar(line: String, sparkSession: SparkSession, tableName: String, threadPool: ExecutorService, prestoTableName: String, frequentlyCarResultTableName: String, zkHostPort: String, cacheDays: Int, prestoHostPort: String) {

    var line_arr = line.split("\\|")

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val map: InputBean = gson.fromJson[InputBean](line_arr(2), mapType)

    val startTime = map.startTime
    val startTimeDateid = TimeUtil.getDateId(map.startTime)

    val fcar = new SparkEngineV2ForFrequentlyCar()

    if (startTimeDateid < getLCacheDataDateid(cacheDays).toInt) {
      println("all time data!!")
      threadPool.execute(new SparkEngineV3ForFrequentlyCarPresto(line, prestoTableName, frequentlyCarResultTableName, zkHostPort, prestoHostPort))
    } else {
      println("cache time data!!")
      fcar.FrequentlyCar(sparkSession, line, tableName, frequentlyCarResultTableName, zkHostPort)
    }
  }

  def SearchCarByPic(actorSystem:ActorSystem,line: String, sparkSession: SparkSession, tableName: String, zkHostPort: String, cacheDays: Int, tableNameAll: String, threadPool: ExecutorService, prestoHostPort: String, prestoTableName: String) {
  
    
    
    var line_arr = line.split("\\|")

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val map: InputBean = gson.fromJson[InputBean](line_arr(2), mapType)

    val startTime = map.startTime
    val startTimeDateid = TimeUtil.getDateId(map.startTime)

    if (startTimeDateid < getLCacheDataDateid(cacheDays).toInt) {
      println("all time data!!")

      threadPool.execute(new SparkEngineV3ForSearchCarByPicPresto(sparkSession, line, prestoTableName, zkHostPort, prestoHostPort))

    } else {
      println("cache time data!!")
      val actor1 = actorSystem.actorOf(Props(new SparkEngineV5ForSearchCarByPic(sparkSession, line, tableName, zkHostPort)), name = "SparkEngineV5ForSearchCarByPicActor")
      actor1 ! "StartSearchCarByPic"
    }

  }

  def SearchSimilarPlate(sparkData: Dataset[Row], line: String, sparkSession: SparkSession, threadPool: ExecutorService, tableName: String, prestoTableName: String, zkHostPort: String, cacheDays: Int, tableNameAll: String, prestoHostPort: String) {
    var line_arr = line.split("\\|")
    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val map: InputBean = gson.fromJson[InputBean](line_arr(2), mapType)

    val startTime = map.startTime
    val startTimeDateid = TimeUtil.getDateId(map.startTime)

    if (startTimeDateid < getLCacheDataDateid(cacheDays).toInt) {
      println("all time data!!")
      threadPool.execute(new SparkEngineV2ForSimilarPlatePresto(line, prestoTableName, zkHostPort, prestoHostPort))
    } else {
      println("cache time data!!")
      val scar = new SparkEngineV2ForSimilarPlate()
      scar.searchSimilarPlateNumber(sparkData, sparkSession, line, tableName, zkHostPort)
    }
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
        if (startTimeDateid < getLCacheDataDateid(cacheDays).toInt)
          allTime = true
      }
    }

    if (allTime) {
      println("all time data!!")
      threadPool.execute(new SparkEngineV2ForMultiPointPresto(line, prestoTableName, zkHostPort, prestoHostPort))
    } else {
      println("cache time data!!")
      val mp = new SparkEngineV2ForMultiPoint()
      mp.searchMultiPoint(sparkData, sparkSession, line, tableName, zkHostPort)
    }

  }

  def CaseCar(sparkData: Dataset[Row], line: String, sparkSession: SparkSession, threadPool: ExecutorService, tableName: String, prestoTableName: String, zkHostPort: String, cacheDays: Int, prestoHostPort: String) {

    var line_arr = line.split("\\|")

    val gson = new Gson
    val mapType = new TypeToken[Array[InputBean]] {}.getType
    val maps: Array[InputBean] = gson.fromJson[Array[InputBean]](line_arr(2), mapType)

    var allTime = false
    maps.foreach { map =>
      {

        val startTime = map.startTime
        val startTimeDateid = TimeUtil.getDateId(map.startTime)
        if (startTimeDateid < getLCacheDataDateid(cacheDays).toInt)
          allTime = true
      }
    }

    if (allTime) {
      println("all time data!!")
      threadPool.execute(new SparkEngineV2ForCaseCarPresto(line, prestoTableName, zkHostPort, prestoHostPort))
    } else {
      println("cache time data!!")
      val mp = new SparkEngineV2ForCaseCar()
      mp.searchCaseCar(sparkData, sparkSession, line, tableName, zkHostPort)
    }

  }

  def EndStation(sparkData: Dataset[Row], line: String, sparkSession: SparkSession, threadPool: ExecutorService, tableName: String, prestoTableName: String, zkHostPort: String, cacheDays: Int, prestoHostPort: String) {

    var line_arr = line.split("\\|")
    val gson = new Gson
    val mapType = new TypeToken[Array[InputBean]] {}.getType
    val maps: Array[InputBean] = gson.fromJson[Array[InputBean]](line_arr(2), mapType)

    var allTime = false
    maps.foreach { map =>
      {

        val startTime = map.startTime
        val startTimeDateid = TimeUtil.getDateId(map.startTime)
        if (startTimeDateid < getLCacheDataDateid(cacheDays).toInt)
          allTime = true
      }
    }

    if (allTime) {
      println("all time data!!")
      threadPool.execute(new SparkEngineV2ForEndStationPresto(line, prestoTableName, zkHostPort, prestoHostPort))
    } else {
      println("cache time data!!")
      val mp = new SparkEngineV2ForEndStation()
      mp.searchEndStation(sparkData, sparkSession, line, tableName, zkHostPort)
    }
  }

  def TogetherCar(sparkSession: SparkSession, line: String, tableName: String, zkHostPort: String) {

    val tc = new SparkEngineV2ForTravelTogether()
    tc.TravelTogether(sparkSession, line, tableName, zkHostPort)

  }

  def ExportLocation(sparkData: Dataset[Row],sparkSession: SparkSession, line: String, tableName: String, zkHostPort: String) {

    val tc = new SparkEngineV2ForExportLocation()
    tc.searchExportLocation(sparkData, sparkSession, line, tableName, zkHostPort)

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