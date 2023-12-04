package com.yisa.engine.test

import com.yisa.engine.common.CommonTaskType
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.util.concurrent.TimeUnit
import org.apache.spark.util.Utils
import com.yisa.engine.db.MySQLConnectionPool
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
import com.yisa.engine.uitl.TimeUtil
import com.google.gson.Gson
import com.yisa.engine.common.InputBean
import com.google.gson.reflect.TypeToken
import java.util.Arrays.ArrayList
import java.util.ArrayList
import com.yisa.engine.common.Locationid_detail
import scala.collection.mutable.ArrayBuffer
import java.util.Base64
import java.util.UUID
import java.util.Random
import java.sql.Timestamp
import scala.collection.immutable.TreeMap
import scala.collection.immutable.HashMap
import scala.collection.mutable.HashSet

object Test1 {

  def main(args: Array[String]): Unit = {
    test36()
  }
  
  
  def test36(){
       for (i <- 0 until 2) {
         print(i)
       }
  }
  
  def test35(){
    
    
    var allLocationid =ArrayBuffer[String]()
    allLocationid += "aa"
    allLocationid += "bb"
    allLocationid.foreach(println)
    
  }
  
  def test34() {
    var aa = new HashMap[String, HashSet[String]]
    aa = aa.+("aa" -> HashSet[String]("aabb"))

    aa("aa").+=("bbcc")

    aa.keys.foreach(aa(_).foreach(println))

  }
  def test33() {

    val tet1 = "001"

    val locationId = Array[String]("aa")

    var l = locationId.reduce((a, b) => a + " OR " + b)
    val query1 = "" + "AND A002:(" + l + " )"
    println(query1)

  }

  //决定系统缓存多少数据，以天为单位
  def getLCacheDataDateid(days: Int): String = {
    var cal = Calendar.getInstance();
    cal.setTime(new Date());
    cal.add(Calendar.DAY_OF_MONTH, -days);
    val format = new SimpleDateFormat("yyyyMMdd")
    format.format(cal.getTime())
  }

  def test32() {

    val yearid = "437"
    val dateid = "2017040614"

    val sBuilder = new StringBuilder();
    sBuilder.append(yearid).append("_").append(dateid);
    val id = sBuilder.toString();
    val rowkey_uuid = UUID.nameUUIDFromBytes(id.getBytes()).toString().replaceAll("-", "");

    println(rowkey_uuid)
  }

  def test31() {
    val useOrNot = "false"
    println(useOrNot.toBoolean)
  }

  def test30() {
    var gs = new Gson
    var query1 = ""

    val test2 = Array[String]("aa", "bb", "cc", "dd", "ee")
    println(gs.toJson(test2))
    if (test2 != null && test2.length != 0 && test2(0) != "" && test2(0) != "0") {
      var l = test2.reduce((a, b) => a + " OR " + b)
      query1 = query1 + " AND A002:(" + l + " )"
    }
    println(query1)
  }

  def test29() {

    val test = ArrayBuffer[String]("1", "1", "2", "3", "4")

    var endResultMap = HashMap[String, String]()

    endResultMap += ("aa" -> "bb")
    println(endResultMap)

  }

  def test28() {
    val ts = 1489941311L
    val random = new Random();
    var num = random.nextInt(999)
    var numS = "";
    if (num.toString().length() == 1) {
      numS = "00" + num
    } else if (num.toString().length() == 2) {
      numS = "0" + num
    }

    var yearid = 449.toString();
    if (yearid.length() == 1) {
      yearid = "000" + num
    } else if (yearid.length() == 2) {
      yearid = "00" + num
    } else if (yearid.length() == 3) {
      yearid = "0" + num
    }

    val version = (ts.toString() + yearid + numS).toLong
    println(version)

  }

  def test27() {

    val test = ArrayBuffer[String]("aa", "bb", "aa")
    val test2 = ArrayBuffer[String]("aa", "bb", "aa")
    val test3 = test.union(test2)
    println(test3)

    println(test.toSet.to)

  }

  def test26() {
    val random = new Random();
    var num = random.nextInt(999)
    var numS = "";
    if (num.toString().length() == 1) {
      numS = "00" + num
    } else if (num.toString().length() == 2) {
      numS = "0" + num
    }
    println(numS)
  }
  def test25() {

    var tt = TreeMap[Long, String]()
    tt + (111L -> "aaaa")

    println(tt.size)

  }

  def test24() {

    val random = new Random();
    println(random.nextInt(999))
  }

  def test23() {

    val times = new Timestamp((148963876500L.toString() + "800000").toLong)
    println((148963876500L.toString() + "800000").toLong)
    println(times)
    println(Long.MaxValue)

  }

  def test22() {

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

  def test21() {
    val timestapVersion = new Date().getTime
    println(timestapVersion)
  }

  def test20() {

    //    println(TimeUtil.getTimeId("20170317092909"))

    println(TimeUtil.getTimestamp("20170317092900").getTime)
    println(TimeUtil.getTimestamp("20170317093000").getTime)
    println(TimeUtil.getTimestamp("20170317093100").getTime)

  }
  def test19() {
    val aaa = "989989000"
    val bbb = aaa.toLong
    println(bbb)
  }
  def test18() {

    var yearids = ArrayBuffer[String]()
    yearids.+=("aaa")

    yearids.foreach { println }

  }
  def test17() {
    val x = "20170309093012"
    val newx = x.substring(0, 8) + "235959"
    println(newx)
  }
  def test16() {
    val data = ArrayBuffer[Int]()
    for (dateid <- 1 to 3) {
      data += dateid
    }

    data.foreach { x => println(x) }
  }
  def test15() {

    val startid = 1;
    val endid = 3;

    val values: List[String] = List("aa", "bb")

    for (dateid <- startid to endid) {
      println(dateid)
    }

    for (a <- 0 to values.size - 1) {
      println(values(a))
    }

  }
  def test14() {

    var gson = new Gson()

    var locationid_detail_List = new ArrayList[Locationid_detail]();

    var ld = Locationid_detail.apply("lo11", 4);
    var ld2 = Locationid_detail.apply("lo22", 4);

    locationid_detail_List.add(ld)
    locationid_detail_List.add(ld2)
    println(ld)
    println(ld2)

    println(gson.toJson(locationid_detail_List))

  }

  def test13() {

    var params = "{'carBrand':'0','carLevel':[],'count':2,'plateNumber':'鄂AX0713','carYear':[],'differ':600,'isRepair':0,'carColor':'','feature':'','locationId':[],'startTime':'20161117000000','endTime':'20161118170000','carModel':[]}"

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val travelTogether: InputBean = gson.fromJson[InputBean](params, mapType)
    println("Ok")
  }
  def test12() {
    var test = Map[String, Int]("a" -> 5, "bb" -> 1)
  }

  def tet11() {

    val line = "{\"carBrand\":\"\",\"count\":2,\"plateNumber\":\"鄂AB62B6\",\"carYear\":[],\"differ\":1,\"isRepair\":0,\"carColor\":\"\",\"feature\":\"\",\"locationId\":[],\"startTime\":\"20161028000000\",\"endTime\":\"20161028105446\"}"

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val map: InputBean = gson.fromJson[InputBean](line, mapType)

    println(map.carModel(0))
    print(map)

    //    println(new Date(1477527252591L).toLocaleString())

  }

  def tet1() {

    var sprakConf = new SparkConf().setAppName("test").setMaster("local").set("spark.sql.warehouse.dir", "file:///D:/spark-warehouse")
    var sc = new SparkContext(sprakConf)
    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL Example")
      .getOrCreate()
    val sqlContext = sparkSession

    import sqlContext.implicits._

    //load data 
    var inittable = sqlContext.read.parquet("hdfs://cdh2:8020/user/hive/warehouse/people_test1")

    inittable.createOrReplaceTempView("perople_test1")

    val resultData = sqlContext.sql("select name from perople_test1")
    resultData.show()

    TimeUnit.SECONDS.sleep(30);

    inittable = sqlContext.read.parquet("hdfs://cdh2:8020/user/hive/warehouse/people_test1")
    inittable.createOrReplaceTempView("perople_test1")

    val resultData2 = sqlContext.sql("select name from perople_test1")
    resultData2.show()
  }

  def tet2() {

    var sprakConf = new SparkConf().setAppName("test").setMaster("local").set("spark.sql.warehouse.dir", "file:///D:/spark-warehouse")
    var sc = new SparkContext(sprakConf)
    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL Example")
      .getOrCreate()
    val sqlContext = sparkSession

    import sqlContext.implicits._

    //load data 
    var inittable = sqlContext.read.parquet("hdfs://cdh1:8020/user/hive/warehouse/people_test2")

    inittable.createOrReplaceTempView("people_test2")

    val resultData = sqlContext.sql("select * from people_test2")
    resultData.show()

  }
  def tet3() {
    var sprakConf = new SparkConf().setAppName("test").setMaster("spark://cdh1:7077").set("spark.sql.warehouse.dir", "file:///D:/spark-warehouse")
    var sc = new SparkContext(sprakConf)
    val warehouseLocation = "hdfs://cdh2:8020/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    // Queries are expressed in HiveQL
    sql("SELECT * FROM people_test2").show()

  }

  def tet5() {

    val t = Array("zookeeper=sichuan1", "groupid=gid111", "isTest=true")

    var test = t.map { t =>
      {

        var line = t.split("=")
        (line(0), line(1))

      }
    }
    test.foreach(println)

  }

  def tet6(x: String) {
    val format = new SimpleDateFormat("yyyyMMddHHmmss")

    val d = format.parse(x);
    println(d.getTime)
    val t = d.getTime / 1000
    println(t)

  }

  def getDateId(x: String): Int = {
    val formatDateid = new SimpleDateFormat("yyyyMMdd")

    try {
      val format = new SimpleDateFormat("yyyyMMddHHmmss")
      val d = format.parse(x);

      return formatDateid.format(d).toInt
    } catch {
      case e: Exception => println("getDateId parse timestamp wrong")
    }
    0

  }
  def tet6() {

    println("1000".toLong)

  }

  def getTimestamp(x: String): Long = {

    val format = new SimpleDateFormat("yyyyMMddHHmmss")

    try {
      if (x == "")
        return 0
      else {
        val d = format.parse(x);
        val t = d.getTime()
        return t
      }
    } catch {
      case e: Exception => println("cdr parse timestamp wrong")
    }

    0

  }

}