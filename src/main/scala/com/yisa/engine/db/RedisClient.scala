package com.yisa.engine.db

import redis.clients.jedis.JedisPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import com.yisa.engine.uitl.ConfigYISA
import java.util.HashSet
import redis.clients.jedis.JedisSentinelPool
import redis.clients.jedis.HostAndPort

object RedisClient extends Serializable {
//  val SENTINEL_HOST = "huangdao1"
//  val SENTINEL_PORT = 26379
//  val SENTINEL_NAME = "mymaster"
//  val set: HashSet[String] = new HashSet[String]();
//
//  val hosts = SENTINEL_HOST.split(",")
//
//  hosts.foreach { host =>
//    {
//      set.add(String.valueOf(new HostAndPort(host, SENTINEL_PORT)))
//
//    }
//  }
//
//  val redisTimeout = 30000
//  var pool: JedisSentinelPool = new JedisSentinelPool(SENTINEL_NAME, set);
//
//  lazy val hook = new Thread {
//    override def run = {
//      println("Execute hook thread: " + this)
//      pool.destroy()
//    }
//  }
//  sys.addShutdownHook(hook.run)
}