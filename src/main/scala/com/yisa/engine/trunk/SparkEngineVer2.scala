package com.yisa.engine.trunk

import org.apache.kafka.clients._
import org.apache.commons.cli._
import java.util.ArrayList
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.util.Date
import java.util.Base64
import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import com.yisa.engine.common.CommonTaskType
import com.yisa.engine.branch.SparkEngineV2ForSearchCarByPic
import java.text.SimpleDateFormat
import com.yisa.engine.common.InputBean
import com.yisa.engine.branch._
import com.yisa.wifi.zookeeper.ZookeeperUtil
import java.util.Calendar
import com.yisa.engine.uitl.TimeUtil
import com.yisa.engine.db.MySQLConnectManager
import com.yisa.engine.branch.SparkEngineV2ForMultiPoint

/**
 * @author liliwei
 * @date  2016年9月9日
 * Spark 基础数据
 */
object SparkEngineVer2 {

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

    //    val zkHostPort = args(0)
    //    val kafkagroupid = args(1)
    //    val test = args(2)
    //properties
    //        val hdfsPath = "hdfs://gpu10:8020" 
    //        val kafka = "gpu3:9092" 
    //        val topic = "SparkEngine"
    //        val frequentlyCarResultTableName =  "pfgc_result_spark"
    val zkUtil = new ZookeeperUtil()
    val configs = zkUtil.getAllConfig(zkHostPort, "spark_engine", false)
    val hdfsPath = configs.get("HDFS_Path")
    val kafka = configs.get("KafkaHostport")
    val refreshData = configs.get("refreshData")
    var topic = ""
    if (test == "debug") {
      topic = configs.get("SparkEngineRequestTopicTest")
    } else {
      topic = configs.get("SparkEngineRequestTopic")
    }
    
    val dataPath1 = configs.get("dataPath1")
    val cacheDays = configs.get("cacheDays").toInt
    val frequentlyCarResultTableName = configs.get("FrequentlyCarJDBCTable")

    //    val sprakConf = new SparkConf();
    //    val sc = new SparkContext(sprakConf)
    var tableName = "pass_info" + getDateMid()

    val sparkSession = SparkSession
      .builder()
      .appName("SparkEngineVer2")
      .getOrCreate()

    //    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //    var sqlContext = sparkSession
    import sparkSession.implicits._

    //load data 
    var inittable2 = loadRealtimeData(sparkSession, hdfsPath, dataPath1, cacheDays)

    inittable2.createOrReplaceTempView(tableName)

//    sparkSession.sql("select * from " + tableName + " order by capturetime desc")

    //    inittable2.cache()

    sparkSession.catalog.cacheTable(tableName)
    

    //    var tableName3Days = "pass_info3days";
    //
    //    var data3days = inittable2.filter("dateid >= " + TimeUtil.getOldDay(2))
    //
    //    data3days.createOrReplaceTempView(tableName3Days)
    //
    //    sparkSession.catalog.cacheTable(tableName3Days)

    // register spark sql udf 
    registerUDF(sparkSession)

    runFirstTask(sparkSession, tableName, frequentlyCarResultTableName, zkHostPort)
    //    runFirstTask(sparkSession, tableName3Days, frequentlyCarResultTableName, zkHostPort)

    //properties for kafka 
    var props = new Properties()
    props.put("bootstrap.servers", kafka)
    props.put("group.id", kafkagroupid)
    props.put("enable.auto.commit", "false")
    props.put("auto.commit.interval.ms", "1000")
    //    props.put("heartbeat.interval.ms", "59999")
    props.put("auto.offset.reset", "latest")
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
    MySQLConnectManager.initConnectionPool(zkHostPort)

    println("start completed!!!!!!!!!!!")

    // read kafka data
    while (true) {
 
      var records = consumer.poll(10)
      var rei = records.iterator()

      //      println("preHour1" + preHour)

      while (rei.hasNext()) {
        //强制让kafka consumer提交同步，以防止后续任务耗时时间过长
        try {
          consumer.commitAsync();
        } catch {
          case e: Exception => {
            println("kafka consumer commitAsync wrong!!!")
            println(e.getMessage)
            println(e.printStackTrace())
            Thread.sleep(300)
            consumer.commitSync();

          }
        }
 
        println("data Version:" + TimeUtil.getStringFromTimestampLong(preHour))
        println("table name :" + tableName)

        val now1 = new Date().getTime()
        var record = rei.next()
        println("line:" + record.value())

        var line = record.value().toString()
        var line_arr = line.split("\\|")

        //        val taskType = line_arr(0).split(",")(0)
        val taskType = line_arr(0)
        //        val taskID = line_arr(0).split(",")(1)
        //        println(taskType)

        try {
          taskType match {

            //频繁过车
            case CommonTaskType.FrequentlyCar => {
              val fcar = new SparkEngineV2ForFrequentlyCar()
              fcar.FrequentlyCar(sparkSession, line, tableName, frequentlyCarResultTableName, zkHostPort)
            }
            //以图搜车
            case CommonTaskType.SearchCarByPic => {
              val fcar = new SparkEngineV2ForSearchCarByPic()
              fcar.SearchCarByPic(sparkSession, line, tableName, zkHostPort)
              //   fcar.SearchCarByPic2(sparkSession, line, tableName, zkHostPort)
              val now3 = new Date().getTime
              println("SearchCarByPic:" + (now3 - now1))
            }
            //相似车牌
            case CommonTaskType.SearchSimilarPlate => {
              val scar = new SparkEngineV2ForSimilarPlate()
              scar.searchSimilarPlateNumber(inittable2, sparkSession, line, tableName, zkHostPort)
            }
            //多点碰撞
            case CommonTaskType.MultiPoint => {

              val mp = new SparkEngineV2ForMultiPoint()
              mp.searchMultiPoint(inittable2, sparkSession, line, tableName, zkHostPort)
            }
            //涉车案件串并
            case CommonTaskType.CaseCar => {

              val mp = new SparkEngineV2ForCaseCar()
              mp.searchCaseCar(inittable2, sparkSession, line, tableName, zkHostPort)
            }
            //终结伪基站
            case CommonTaskType.EndStation => {

              val mp = new SparkEngineV2ForEndStation()
              mp.searchEndStation(inittable2, sparkSession, line, tableName, zkHostPort)
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
        }
      }
 
      // reload data
      val nowTime = new Date().getTime
      if ((nowTime - preHour) > refreshData.toLong) {
        println("start load data-------------preHour:" + preHour + "---------now:" + getDateMid() + "--------------------------------------------------")
        var loadDataStart = new Date().getTime
        //              tableName = "pass_info" + getDateMid()

        sparkSession.catalog.uncacheTable(tableName)
        sparkSession.catalog.dropTempView(tableName)
        sparkSession.catalog.clearCache()
        Thread.sleep(1000)

        //load data 1
        tableName = "pass_info" + nowTime
        inittable2 = loadRealtimeData(sparkSession, hdfsPath, dataPath1, cacheDays)
        inittable2.createOrReplaceTempView(tableName)

        sparkSession.catalog.cacheTable(tableName)
        runFirstTask(sparkSession, tableName, frequentlyCarResultTableName, zkHostPort)
        preHour = new Date().getTime

        var loadDataEnd = preHour
        println("end load data-------continued for about :" + (loadDataEnd - loadDataStart) + "  m------------------------------------------------------")
      }
    }
  }

  def loadData(sqlContext: SparkSession, hdfsPath: String): Dataset[Row] = {

    //    val inittable = sqlContext.read.parquet(hdfsPath + "/moma/pass_info09013000wan")
    //    val inittable_pass_info_merge = sqlContext.read.parquet(hdfsPath + "/user/hive/warehouse/yisadata.db/pass_info_merge")
    //    		val inittable_pass_info_merge = sqlContext.read.parquet(hdfsPath + "/user/hive/warehouse/yisadata.db/pass_infotest/pass_info_merge.parquet")
    val inittable_pass_info_merge = sqlContext.read.option("mergeSchema", "true").parquet(hdfsPath + "/parquet/pass_info_merge")
    val inittable_pass_info_merge2 = inittable_pass_info_merge.filter("plateNumber not like  '%无%' ").filter("plateNumber !=  '0' ").filter("plateNumber not like  '%未%' ")

    return inittable_pass_info_merge2

  }

  def loadRealtimeData(sparkSession: SparkSession, hdfsPath: String, dataPath: String, cacheDays: Int): Dataset[Row] = {

    val loadData = sparkSession.read.option("mergeSchema", "true").parquet(hdfsPath + dataPath)
    val loadData_filter = loadData
      .filter("platenumber !=  '0' ")
//      .filter("")
//      .filter("platenumber not like  '%未%' ")
      .filter { x => x.getAs[String]("platenumber").length() > 1 }
      .filter { x => x.getAs[String]("platenumber").length() < 20 }
      .filter("dateid >= " + getLCacheDataDateid(cacheDays))

    loadData_filter.createOrReplaceTempView("tmp")

    val loadData_filter2 = sparkSession.sql("select * from tmp  order by capturetime desc")

    return loadData_filter2
  }

  def getDateMid(): Long = {

    val format = new SimpleDateFormat("yyyyMMddHHmm")
    val date_S = format.format(new Date())
    date_S.substring(0, date_S.size - 2).toLong

  }

  def runFirstTask(sparkSession: SparkSession, tableName: String, frequentlyCarResultTableName: String, zkHostPort: String): Unit = {

    val fcar = new SparkEngineV2ForFrequentlyCar()
    fcar.FrequentlyCar(sparkSession, "01|test1|{\"count\":2}", tableName, frequentlyCarResultTableName, zkHostPort)

  }

  def registerUDF(sparkSession: SparkSession): Unit = {

    //以图搜车，相似度 by 李立伟
    sparkSession.udf.register("getSimilarity", (text: String, test2: String) => {
      val byteData = Base64.getDecoder.decode(text)
      val oldData = Base64.getDecoder.decode(test2)
      var num: Long = 0
      for (i <- 0 until byteData.length if test2.length > 30) {
        var n = (byteData(i) & 0xff) - (oldData(i) & 0xff)
        num += n * n;
      }
      num
    })

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
 