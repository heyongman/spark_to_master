package com.he.client

import com.he.core.{Borrow, Logging}
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings, DB}

object DbPool extends Borrow with Logging {

  def init(config: Map[String,String]): Unit ={
    val poolName = Symbol(config.getOrElse("poolName", "default"))
    if(!ConnectionPool.isInitialized(poolName)){
      Class.forName(config.getOrElse("driver","com.mysql.cj.jdbc.Driver"))
      val settings = ConnectionPoolSettings(
        initialSize = 0,
        maxSize = 1,
        connectionTimeoutMillis = 3000L,
        validationQuery = "select 1")
      ConnectionPool.add(poolName,config.getOrElse("url",null),
        config.getOrElse("username",null),config.getOrElse("password",null),settings)
      info(s"Initialize db pool: $poolName")
      sys.addShutdownHook(ConnectionPool.close(poolName))
    }
  }

  //使用默认连接池
  def init(poolName:String, driver:String, url:String, username:String, password:String): Unit = {
    if(!ConnectionPool.isInitialized(Symbol(poolName))){
      Class.forName(driver)
      val settings = ConnectionPoolSettings(
        initialSize = 0,
        maxSize = 1,
        connectionTimeoutMillis = 3000L,
        validationQuery = "select 1")
      ConnectionPool.add(Symbol(poolName),url,username,password,settings)
      info(s"Initialize db pool: $poolName")
      sys.addShutdownHook(ConnectionPool.close(poolName))
    }
  }

  def usingDB[A](poolName:String)(execute:DB => A) :A = using(DB(ConnectionPool(Symbol(poolName)).borrow()))(execute)
}
