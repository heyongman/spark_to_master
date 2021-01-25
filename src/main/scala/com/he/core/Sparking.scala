package com.he.core

import com.he.listener.{SparkCoreListener, SparkSqlListener}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

trait Sparking {

  self:{ def main(args: Array[String]):Unit } =>

  val conf = new SparkConf()
  conf.set("spark.serializer", classOf[KryoSerializer].getName)
  conf.set("spark.extraListeners", classOf[SparkCoreListener].getName)
  conf.setAppName(this.getClass.getName.stripSuffix("$"))

  def enableLocalSupport(): Unit = conf.setMaster("local[*]")

  def enableSqlBloodSupport(): Unit = conf.set("spark.sql.queryExecutionListeners", classOf[SparkSqlListener].getName)

  def enableTaskMonitorSupport(): Unit = conf.set("enableSendMessageOnTaskFail", "true")

  def getSparkSession(uris: Option[String]): SparkSession = {
    val builder = SparkSession.builder().config(conf)
    if(uris.isDefined){
      builder
        .config("hive.metastore.uris", uris.get)
        .enableHiveSupport()
    }
    builder.getOrCreate()
  }
}
