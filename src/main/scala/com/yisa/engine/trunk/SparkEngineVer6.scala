package com.yisa.engine.trunk

import java.text.SimpleDateFormat
import java.util.ArrayList
import java.util.Base64
import java.util.Calendar
import java.util.Date
import java.util.Properties
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import org.apache.commons.cli._
import org.apache.kafka.clients._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession

import com.google.gson.Gson
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
import com.yisa.engine.modules.SparkEngineModulesForVer21
import com.yisa.engine.spark.udf.getSolridByMaxCapturetime
import com.yisa.engine.spark.udf.ImageSimilarity
import com.yisa.engine.db.RedisConnectionPool
import com.yisa.engine.uitl.ConfigYISA
import com.yisa.engine.db.RedisClusterClientZK
import scala.collection.mutable.ArrayBuffer
import com.yisa.engine.modules.SparkEngineModulesForVer5Alluxio
import com.yisa.engine.db.HBaseConnectManager

/**
 * @author liliwei
 * @date  2016年9月9日
 * Spark 基础数据
 */
object SparkEngineVer6 {

  def main(args: Array[String]): Unit = {

    var cmd: CommandLine = null
    val options = new Options()
    try {
      val zk = new Option("zookeeper", true, "zookeeper host and port.Example 10.0.0.1:2181")
      zk.setRequired(true);
      options.addOption(zk);

      val kafkaConsumerID = new Option("kafkaConsumer", true, "kafka Consumer groupid,recommend non-repeated mark ");
      kafkaConsumerID.setRequired(true);
      options.addOption(kafkaConsumerID);

      val isTest = new Option("isTest", true, "This Program is test or not,1 means beta  version,other means official versionm,if set it 1 ,please stop the program after test");
      isTest.setRequired(true);
      options.addOption(isTest);

      val parser = new PosixParser();
      cmd = parser.parse(options, args);
    } catch {
      case e: Exception => {
        println("Submit SparkEngine Error!!!!!!!!!!!!!!!!!!")
        println(e.getMessage)
        println(e.printStackTrace())
        val formatter = new HelpFormatter()
        formatter.printHelp("This program args list:", options);
        System.exit(1)
      }
    }
    val zkHostPort = cmd.getOptionValue("zookeeper");
    val kafkagroupid = cmd.getOptionValue("kafkaConsumer");
    val test = cmd.getOptionValue("isTest");

    ConfigYISA.initConfig(zkHostPort)

    val zkUtil = new ZookeeperUtil()
    val configs = zkUtil.getAllConfig(zkHostPort, "spark_engine", false)
    val hdfsPath = configs.get("HDFS_Path")
    println("hdfsPath:" + hdfsPath)
    val dataPath1 = configs.get("dataPath1")
    val alluxioPath = configs.get("alluxio_Path")
    println("alluxioPath:" + alluxioPath)
    val dataPathAlluxio = configs.get("alluxio_data_Path")

    println("alluxio_data_Path:" + dataPathAlluxio)
    println("dataPath1:" + dataPath1)

    val kafka = configs.get("KafkaHostport")
    println("kafka:" + kafka)
    val refreshData = configs.get("refreshData")
    println("refreshData:" + refreshData)
    val prestoHostPort = configs.get("PrestoHostPort")
    println("prestoHostPort:" + prestoHostPort)
    //use redis as search car memory-index or not
    val useRedisOrNot = configs.get("useRedisOrNot").toBoolean
    println("useRedisOrNot:" + useRedisOrNot)
    if (useRedisOrNot) {
      RedisClusterClientZK.getJedisCluster(zkHostPort)
    }

    var topic = ""
    println("test:" + test)
    if (test == "debug") {
      topic = configs.get("SparkEngineRequestTopicTest")
    } else {
      topic = configs.get("SparkEngineRequestTopic")
    }

    val prestoTableName = configs.get("PrestoTableName")

    val cacheDays = configs.get("cacheDays").toInt
    println("cacheDays:" + cacheDays)
    val cacheDays4SearchCar = configs.get("cacheDays4SearchCar").toInt
    println("cacheDays4SearchCar:" + cacheDays4SearchCar)
    val frequentlyCarResultTableName = configs.get("FrequentlyCarJDBCTable")
    println("frequentlyCarResultTableName:" + frequentlyCarResultTableName)

    val solr_url = configs.get("solr_url")

    println("listen topic :" + topic)
    println("cache Data Time:" + cacheDays)
    println("prestoTableName:" + prestoTableName)

    var tableName = "pass_info" + getDateMid()
    var tableNameAll = "pass_info_all"

    MySQLConnectManager.initConnectionPool(zkHostPort)
    HBaseConnectManager.initConnet(zkHostPort)

    import scala.collection.mutable.Set
    import scala.collection.mutable.Map
    val allYearidByBrandid: Map[Int, Set[Long]] = MySQLConnectManager.getAllYearidByBrandid(zkHostPort);
    val allYearidByModelid: Map[Int, Set[Long]] = MySQLConnectManager.getAllYearidByModelID(zkHostPort);
    val allLocations: ArrayBuffer[String] = MySQLConnectManager.getAllLocationid(zkHostPort);

    
    
    val AllYearids = ArrayBuffer[String]()
    if (AllYearids.size <= 0) {
      allYearidByBrandid.foreach(yearidsByBrandid => {
        yearidsByBrandid._2.foreach { yearid => AllYearids.+=(yearid.toString()) }
      })
    }

    println("allYearidByBrandid size:" + allYearidByBrandid.size)
    println("allYearidByModelid size:" + allYearidByModelid.size)
    println("AllYearids size:" + AllYearids.size)
    println("allLocations size:" + allLocations.size)

    val sparkSession = SparkSession
      .builder()
      .appName("SparkEngine")
      //      .config("spark.locality.wait", "1s")
      .config("spark.scheduler.mode", "FAIR")
      .getOrCreate()

    //load data 
//    var inittable2 = loadRealtimeData(sparkSession, alluxioPath, dataPathAlluxio, cacheDays)
//    inittable2.createOrReplaceTempView(tableName)

//    var loadAllData = loadAlltimeData(sparkSession, hdfsPath, dataPath1, cacheDays)
//    loadAllData.createOrReplaceTempView(tableNameAll)

    // register spark sql udf 
    registerUDF(sparkSession)

//    runFirstTask(sparkSession, tableName, frequentlyCarResultTableName, zkHostPort)

    //properties for kafka 
    var props = new Properties()
    props.put("bootstrap.servers", kafka)
    props.put("group.id", kafkagroupid)
    props.put("enable.auto.commit", "false")
    //    props.put("auto.commit.interval.ms", "1000")
    //    props.put("heartbeat.interval.ms", "59999")
    //    props.put("auto.offset.reset", "latest")
    //    props.put("session.timeout.ms", "60000")
    //    props.put("request.timeout.ms", "60001")
    //    props.put("group.max.session.timeout.ms", "600000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    var consumer = new KafkaConsumer(props)
    //kafka topic name 
    var topics = new ArrayList[String]()
    topics.add(topic)
    consumer.subscribe(topics)

    var preHour = new Date().getTime()

    println("start completed!!!!!!!!!!!")

    // read kafka data
    while (true) { 
 
      var records = consumer.poll(100)
      var rei = records.iterator()

      //强制让kafka consumer提交同步
      try {
        consumer.commitSync();

      } catch {
        case e: Exception => {
          println("kafka consumer commitAsync wrong!!!")
          println(e.getMessage)
          println(e.printStackTrace())
          Thread.sleep(300)
          consumer.commitAsync();

        }
      }

      while (rei.hasNext()) {

        println("data Version:" + TimeUtil.getStringFromTimestampLong(preHour))
        println("table name :" + tableName)

        val now1 = new Date().getTime()
        var record = rei.next()
        println("line:" + record.value())
        var line = record.value().toString()
        var line_arr = line.split("\\|")
        val taskType = line_arr(0)

        //创建线程池
        val threadPool: ExecutorService = Executors.newFixedThreadPool(1)

        val cacheDays_2 = cacheDays

        val SparkEngineModules = SparkEngineModulesForVer5Alluxio

        try {
          taskType match {

            //频繁过车
            case CommonTaskType.FrequentlyCar => {

              //              SparkEngineModulesForVer21.FrequentlyCar(line, sparkSession, tableName, threadPool, prestoTableName, frequentlyCarResultTableName, zkHostPort, cacheDays, prestoHostPort)
              SparkEngineModules.FrequentlyCar(line, sparkSession, tableName, threadPool, prestoTableName, frequentlyCarResultTableName, zkHostPort, cacheDays_2, prestoHostPort,allLocations)

            }
            //以图搜车
            case CommonTaskType.SearchCarByPic => {
              SparkEngineModules.SearchCarByPic(line, sparkSession, tableName, zkHostPort, cacheDays4SearchCar, threadPool, prestoHostPort, prestoTableName, allYearidByBrandid, allYearidByModelid, AllYearids, useRedisOrNot)

            }
            //相似车牌
            case CommonTaskType.SearchSimilarPlate => {

              //            	SparkEngineModulesForVer21.SearchSimilarPlate(inittable2, line, sparkSession, threadPool, tableName, prestoTableName, zkHostPort, cacheDays, tableNameAll, prestoHostPort)
              SparkEngineModules.SearchSimilarPlate(null, line, sparkSession, threadPool, tableName, prestoTableName, zkHostPort, cacheDays_2, prestoHostPort)
            }
            //多点碰撞
            case CommonTaskType.MultiPoint => {
              //              SparkEngineModulesForVer21.MultiPoint(inittable2, line, sparkSession, threadPool, tableName, prestoTableName, zkHostPort, cacheDays, prestoHostPort)
              SparkEngineModules.MultiPoint(null, line, sparkSession, threadPool, tableName, prestoTableName, zkHostPort, cacheDays_2, prestoHostPort)

            }
            //涉车案件串并
            case CommonTaskType.CaseCar => {

              //              SparkEngineModulesForVer21.CaseCar(inittable2, line, sparkSession, threadPool, tableName, prestoTableName, zkHostPort, cacheDays, prestoHostPort)
              SparkEngineModules.CaseCar(null, line, sparkSession, threadPool, tableName, prestoTableName, zkHostPort, cacheDays_2, prestoHostPort)

            }
            //终结伪基站
            case CommonTaskType.EndStation => {

              //            	SparkEngineModulesForVer21.EndStation(inittable2, line, sparkSession, threadPool, tableName, prestoTableName, zkHostPort, cacheDays, prestoHostPort)
              SparkEngineModules.EndStation(null, line, sparkSession, threadPool, tableName, prestoTableName, zkHostPort, cacheDays_2, prestoHostPort)

            }
            //同行车辆
            case CommonTaskType.TogetherCar => {

              SparkEngineModules.TogetherCar(sparkSession, line, tableName, zkHostPort, solr_url)

            }
            //导出卡口
            case CommonTaskType.ExportLocation => {

              SparkEngineModules.ExportLocation(null, sparkSession, line, tableName, zkHostPort)

            }

            case CommonTaskType.Nocturnal => {

              SparkEngineModules.ConcealOneselfByDayAndMarchByNight(line, sparkSession, tableName, zkHostPort, cacheDays, threadPool, prestoHostPort, prestoTableName)
            }
          }
          val now2 = new Date().getTime
          println("Task Total time:" + (now2 - now1))
        } catch {
          case e: Exception => {
            println("task wrong!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            println(e.getMessage)
            println(e.printStackTrace())
          }
        } finally {
          println("stop threadPool ")
          threadPool.shutdown()
        }

      }

      // reload data
//      val nowTime = new Date().getTime
//      if ((nowTime - preHour) > refreshData.toLong) {
//        println("start load data-------------preHour:" + preHour + "---------now:" + getDateMid() + "--------------------------------------------------")
//        var loadDataStart = new Date().getTime
//        sparkSession.catalog.refreshTable(tableName)
//
//        preHour = new Date().getTime
//        var loadDataEnd = preHour
//        println("end load data-------continued for about :" + (loadDataEnd - loadDataStart) + " ------------------------------------------------------")
//      }
    }
  }

 

  def getDateMid(): Long = {

    val format = new SimpleDateFormat("yyyyMMddHHmm")
    val date_S = format.format(new Date())
    date_S.substring(0, date_S.size - 2).toLong

  }

  def runFirstTask(sparkSession: SparkSession, tableName: String, frequentlyCarResultTableName: String, zkHostPort: String): Unit = {

    sparkSession.sql("select count(*) from " + tableName).show()

  }

  def registerUDF(sparkSession: SparkSession): Unit = {

    // 多点碰撞，max_by实现
    sparkSession.udf.register("max_by", new getSolridByMaxCapturetime)

    //以图搜车，相似度 by 李立伟
    sparkSession.udf.register("getSimilarity", (text: String, test2: String) => new ImageSimilarity().getSimilarity(text, test2))

    //
    //    //以图搜车，相似度 by 李立伟
    //    sparkSession.udf.register("getSimilarity", (text: String, test2: String) =>   {
    //      val byteData = Base64.getDecoder.decode(text)
    //      val oldData = Base64.getDecoder.decode(test2)
    //      var num: Long = 0
    //      var out:Int = 1 
    //      for ( i <- 0 until byteData.length if test2.length > 30 && out ==1) {
    //        var n = (byteData(i) & 0xff) - (oldData(i) & 0xff)
    //        num += n * n;
    //        if (num > 20000) {
    //          
    //        
    //        }
    //
    //      }
    //      num
    //    })

    //by liudong 相似车牌 
    sparkSession.udf.register("getSimilay", (s1: String, s2: String) => {

      val len1 = s1.length()
      val len2 = s2.length()
      var len: Int = 0
      if (len1 != len2 || (s1.charAt(0) != s2.charAt(0))) {
        len = 10
      } else {

        for (i <- 1 until len1) {

          if (s1.charAt(i) != s2.charAt(i)) {
            //              if(len > 2){
            //                len = 10
            //              }
            len = len + 1
          }
        }
      }
      len
    })

    //by liudong 相似车牌 ,编辑距离算法
    sparkSession.udf.register("levenshtein", (s: String, t: String) => {
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
    })
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
 