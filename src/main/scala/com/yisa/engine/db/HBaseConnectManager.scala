package com.yisa.engine.db

import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.conf.Configuration
import com.yisa.wifi.zookeeper.ZookeeperUtil

/**
 * @author liliwei
 * @date  2016年9月9日
 * HBase 连接池
 */
object HBaseConnectManager extends Serializable {
  @volatile private var conn: Connection = null;
  @volatile private var config: Configuration = null;
  private val hbase_zookeeper_property_clientPort = "2181"
  private var hbase_zookeeper_quorum = "wh3"

  def getConfig(zkHostport: String): Configuration = {

    if (config == null) {
      synchronized {
        if (config == null) {
          try {

            val zkUtil = new ZookeeperUtil()
            val configs = zkUtil.getAllConfig(zkHostport, "spark_engine", false)
            val ZK_HOSTS = configs.get("ZK_HOSTS")
            hbase_zookeeper_quorum = ZK_HOSTS
            config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.property.clientPort", hbase_zookeeper_property_clientPort);
            config.set("hbase.zookeeper.quorum", hbase_zookeeper_quorum);
          } catch {
            case ex: Exception => {
              ex.printStackTrace()
              println("HBaseConnectManager Exception")
            }
          }
        }
      }
    }
    config
  }

  def getConnet(zkHostport: String): Connection = {

    if (conn == null || conn.isClosed()) {
      synchronized {
        if (conn == null || conn.isClosed()) {

          println("initConnection  HBaseConnectManager ---ing")
          val zkUtil = new ZookeeperUtil()
          val configs = zkUtil.getAllConfig(zkHostport, "spark_engine", false)
          val ZK_HOSTS = configs.get("ZK_HOSTS")
          hbase_zookeeper_quorum = ZK_HOSTS
          val configuration = HBaseConfiguration.create();
          configuration.set("hbase.zookeeper.property.clientPort", hbase_zookeeper_property_clientPort);
          configuration.set("hbase.zookeeper.quorum", hbase_zookeeper_quorum);
          try {
            conn = ConnectionFactory.createConnection(configuration)
          } catch {
            case ex: Exception => {
              ex.printStackTrace()
              println("HBaseConnectManager Exception")
            }
          }
          println("initConnection  HBaseConnectManager ---completed")
        }
      }
    }
    conn
  }

  def initConnet(zkHostport: String): Connection = {

    if (conn == null || conn.isClosed()) {
      synchronized {
        if (conn == null || conn.isClosed()) {

          println("initConnection  HBaseConnectManager ---ing")
          val zkUtil = new ZookeeperUtil()
          val configs = zkUtil.getAllConfig(zkHostport, "spark_engine", false)
          val ZK_HOSTS = configs.get("ZK_HOSTS")
          hbase_zookeeper_quorum = ZK_HOSTS
          val configuration = HBaseConfiguration.create();
          configuration.set("hbase.zookeeper.property.clientPort", hbase_zookeeper_property_clientPort);
          configuration.set("hbase.zookeeper.quorum", hbase_zookeeper_quorum);
          try {
            conn = ConnectionFactory.createConnection(configuration)
          } catch {
            case ex: Exception => {
              ex.printStackTrace()
              println("HBaseConnectManager Exception")
            }
          }
          println("initConnection  HBaseConnectManager ---completed")
        }
      }
    }
    conn
  }
}