package com.yisa.engine.uitl

import java.util.Properties
import java.io.FileInputStream
import com.yisa.wifi.zookeeper.ZookeeperUtil
import java.util.HashMap

object ConfigYISA {

  // zookeeper工具类
  var zkUtil: ZookeeperUtil = null
  var ZK_SERVER_UT = ""

  var configs: java.util.Map[String, String] = new java.util.HashMap[String, String]()

  val ZK_TYPE = "spark_engine"


  // 初期化配置文件
  def initConfig(zkServer: String) {

    if (zkUtil == null) {

      ZK_SERVER_UT = zkServer

      zkUtil = new ZookeeperUtil()

      configs = zkUtil.getAllConfig(ZK_SERVER_UT, "spark_engine", false)

    }
  }

}