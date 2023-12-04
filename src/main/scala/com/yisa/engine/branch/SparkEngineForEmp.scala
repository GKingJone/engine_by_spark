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

/**
 * @author liliwei
 * @date  2016年9月9日
 * 以图搜车
 */
class SparkEngineForEmp {

  def SearchCarByPic(sqlContext: SQLContext, line: String, tableName: String,zkHostport:String): Unit = {
   
    println("countTest")
  }
}

