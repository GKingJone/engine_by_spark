package com.yisa.engine.other

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.util.Date
import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients._
import java.util.ArrayList

object KafkaTest {

  def main(args: Array[String]): Unit = {
    //properties
    val kafka = "bigdata1:9092"
    val topic = "test"
    val kafkagroupid = "test12"

    //properties for kafka 
    var props = new Properties()
    props.put("bootstrap.servers", kafka)
    props.put("group.id", kafkagroupid)
    props.put("enable.auto.commit", "true")
    //    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", "latest")
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
 

      }

    }

  }
}