package com.yisa.engine.other

import com.yisa.engine.common.CommonTaskType
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.util.concurrent.TimeUnit
import org.apache.spark.util.Utils
object Test1 {

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
//    var inittable = sqlContext.read.parquet("hdfs://cdh2:8020/user/hive/warehouse/people_test2")
    var inittable = sqlContext.read.parquet("hdfs://cdh2:8020/moma/test1/")

    inittable.createOrReplaceTempView("people_test2")
    inittable.printSchema()

    val resultData = sqlContext.sql("select * from people_test2 limit 1 ")
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

  def main(args: Array[String]): Unit = {
    tet2()
  }

}