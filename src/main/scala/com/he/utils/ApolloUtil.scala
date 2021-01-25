package com.he.utils

import com.ctrip.framework.apollo.{Config, ConfigService}

object ApolloUtil {
  System.setProperty("app.id", "gy-tocc-process")
  System.setProperty("apollo.meta", "http://10.11.56.12:8080")
  val config: Config = ConfigService.getConfig("application")

  val kafkaServer: String = config.getProperty("kafka.bootstrap.server", null)
  val zkServer: String = config.getProperty("kafka.zookeeper.server", null)
  val topic: Array[String] = config.getProperty("kafka.test.topic", null).split(",")
  val group: String = config.getProperty("kafka.test.group", null)

}
