package com.he.utils

import java.io.InputStream
import java.util.Properties

object Config {
  private val stream: InputStream = Config.getClass.getClassLoader.getResourceAsStream("my.properties")
  private val prop = new Properties()
  prop.load(stream)

  val mysqlLocalName = "local"
  val mysqlLocal = Map(
    "url" -> prop.getProperty("jdbc.url"),
    "driver" -> prop.getProperty("jdbc.driver"),
    "user" -> prop.getProperty("jdbc.user"),
    "password" -> prop.getProperty("jdbc.password"),
    "poolName" -> mysqlLocalName
  )

//  修改或添加：val newMap = mutable.Map(Config.es.toSeq: _*) += ("es.nodes" -> "hadoop01:9092","key" -> "value")
  val es = Map(
    "es.nodes" -> "10.197.236.193:9200,10.197.236.194:9200,10.197.236.200:9200,10.197.236.201:9200"
  )

}
