package com.he.client

import com.he.core.{Borrow, Logging}
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

object RedisPool extends Borrow with Logging{
  var pool:JedisPool = _

  /**
   * 初始化
   * @param host
   * @param port
   */
  def init(host:String,port:Int): Unit ={
    if (pool == null){
      pool = new JedisPool(new GenericObjectPoolConfig(), host, port)
      log.info("Initialize redis pool: {}",pool)
      sys.addShutdownHook(pool.destroy())
    }
  }

  /**
   * 初始化
   * @param host
   * @param port
   */
  def init(host:String,port:Int,timeOut:Int): Unit ={
    if (pool == null){
      pool = new JedisPool(new GenericObjectPoolConfig(), host, port, timeOut)
      sys.addShutdownHook(pool.destroy())
    }
  }


  def usingRedis[A](execute:Jedis=>A):A = using(pool.getResource)(execute)

}
