package com.yisa.engine.branchV5

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.util.Base64
import org.apache.spark.sql.SparkSession
import com.yisa.engine.db.MySQLConnectManager
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.yisa.engine.common.InputBean
import com.yisa.engine.uitl.TimeUtil
import java.util.Date
import akka.actor.Actor
import com.yisa.engine.branch.SparkEngineV2ForSearchCarByPic

/**
 * @author liliwei
 * @date  2017年2月17日
 * 以图搜车
 */
class SparkEngineV5ForSearchCarByPic(sqlContext: SparkSession, line: String, tableName: String, zkHostport: String) extends Actor {

  var sparkSession: SparkSession = _

  override def preStart(): Unit = {
    sparkSession = sqlContext.newSession()
  }

  override def postStop(): Unit = sparkSession.stop()
  
  override def receive: Receive = {
    case n: String =>{
      val fcar = new SparkEngineV2ForSearchCarByPic()
      fcar.SearchCarByPic(sparkSession, line, tableName, zkHostport)
    }
  }
}
 




  





