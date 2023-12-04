package com.yisa.engine.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.util.Date
import java.util.Base64
import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients._
import java.util.ArrayList
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.mysql.jdbc.Driver

object SparkSQLKafka {

  def main(args: Array[String]): Unit = {
    //properties

    val hdfsPath = "hdfs://gpu10:8020"
    val kafka = "gpu3:9092"
    val topic = "test"
    val kafkagroupid = "test"
    val jdbcurl = "jdbc:mysql://128.127.120.200:3306/wifi_pass?useUnicode=true&characterEncoding=UTF-8"
    val jdbcTable = "SearchCar"
    val jdbcUser = "root"
    val jdbcPwd = "root"

    // jdbc 
    val connectionProperties = new Properties();
    connectionProperties.put("user", jdbcUser)
    connectionProperties.put("password", jdbcPwd)
    //    connectionProperties.put("driver", "com.mysql.jdbc.Driver")

    var sprakConf = new SparkConf().setAppName("test");
    var sc = new SparkContext(sprakConf)

    sc.getConf.set("spark.executor.extraClassPath", "/moma/mysql-connector-java-5.1.39.jar");

    //    val sparkSession = SparkSession
    //      .builder()
    //      .appName("Spark SQL Example")
    //      .getOrCreate()
    // val sqlContext = sparkSession
    val sqlContext = new SQLContext(sc);

    import sqlContext.implicits._

    //load data 
    val inittable = sqlContext.read.parquet(hdfsPath + "/moma/pass_info0824800wan")

    //register table for spark sql  
    inittable.createOrReplaceTempView("passinfo")

    inittable.cache()

    // register spark sql udf 
    sqlContext.udf.register("getSimilarity", (text: String, test2: String) => {
      val byteData = Base64.getDecoder.decode(text)
      val oldData = Base64.getDecoder.decode(test2)
      var num: Long = 0
      for (i <- 0 until byteData.length if test2.length > 30) {
        var n = (byteData(i) & 0xff) - (oldData(i) & 0xff)
        num += n * n;
      }
      num
    })

    //properties for kafka 
    var props = new Properties()
    props.put("bootstrap.servers", kafka)
    props.put("group.id", kafkagroupid)
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    var consumer = new KafkaConsumer(props)
    //kafka topic name 
    var topics = new ArrayList[String]()
    topics.add(topic)
    consumer.subscribe(topics)

    // read kafka data
    while (true) {

      var records = consumer.poll(100)
      var rei = records.iterator()

      while (rei.hasNext()) {
        val now3 = new Date().getTime()
        var record = rei.next()
        //        println("line:",record.offset(), record.key(), record.value())
        println("line:" + record.value())
        var line = record.value().toString()
        var line_arr = line.split(",")
        val feature = line_arr(0)
        val modelId = line_arr(1)
        val brandId = line_arr(2)

        val teenagers = sqlContext.sql("SELECT id  FROM passinfo where  recFeature != '' and recFeature != 'null' and brandId = " + brandId + "  and modelId  = " + modelId + " and recFeature ='" + feature + "'")
        //        teenagers.map(t => "id: " + t(0)).collect().foreach(println)
        teenagers.foreach { t =>
          
          println("id: " + t(0))
        }
        val now4 = new Date().getTime()
        teenagers.write.mode(SaveMode.Append).jdbc(jdbcurl, jdbcTable, connectionProperties)
        val now5 = new Date().getTime()
        val teenagers2 = sqlContext.sql("SELECT id,getSimilarity('" + feature + "',recFeature) as similarity FROM passinfo where  recFeature != '' and recFeature != 'null' and brandId = " + brandId + "  and modelId  = " + modelId + " order by similarity  limit 100")
        teenagers2.foreach(t => {

          println("id: " + t(0) + " similarity:" + t(1))
          val now = new Date().getTime()
          println(now - now4)
        })
        val now6 = new Date().getTime()
        teenagers2.write.mode(SaveMode.Append).jdbc(jdbcurl, jdbcTable, connectionProperties)

        val now7 = new Date().getTime()
       
        
        
        
       val teenagers3 = sqlContext.sql("SELECT id,getSimilarity('" + feature + "',recFeature) as similarity FROM passinfo where  recFeature != '' and recFeature != 'null' and brandId = " + brandId + "  and modelId  = " + modelId + " order by similarity  limit 10000")
       teenagers3.write.mode(SaveMode.Append).jdbc(jdbcurl, jdbcTable, connectionProperties)
        val now8 = new Date().getTime()
        
        
         println("first:"+(now4 - now3))
        println("first to mysql:"+(now5 - now4))
        println("second:"+(now6 - now5))
        println("second to mysql :"+(now7 - now6))
        println("10 thousand to mysql  :"+(now8 - now7))
        
      }

    }

  }
}