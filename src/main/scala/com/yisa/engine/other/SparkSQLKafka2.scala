package com.yisa.engine.other

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.util.Date
import java.util.Properties
import org.apache.kafka.clients._
import org.apache.spark.SparkConf

object SparkSQLKafka2 {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    //properties

    val hdfsPath = "hdfs://gpu10:8020"
    val kafka = "gpu3:9092"
    val topic = "test"
    val kafkagroupid = "test"
    val jdbcurl = "jdbc:mysql://bigdata1:3306/test"
    val jdbcTable = "test"
    val jdbcUser = "root"
    val jdbcPwd = "root"

    // jdbc 
    val connectionProperties = new Properties();
    connectionProperties.put("user", jdbcUser)
    connectionProperties.put("password", jdbcPwd)

    var sprakConf = new SparkConf().setAppName("test").setMaster("local");
    var sc = new SparkContext(sprakConf)

    //    val sparkSession = SparkSession
    //      .builder()
    //      .appName("Spark SQL Example")
    //      .getOrCreate()
    // val sqlContext = sparkSession
    val sqlContext = new SQLContext(sc);

    var now = new Date().getTime()
    //  var sprakConf = new SparkConf().setAppName("sparksql");

    //    val sqlContext = sparkSession

//    val parquetFile = sqlContext.read.parquet("hdfs://bigdata1:9000/moma/pass_info0816")
    
    var usersDf = sqlContext.read.json("E://moma//test.json")
    
  


    usersDf.write.jdbc(jdbcurl, jdbcTable, connectionProperties)

  }
}