package com.he.kafka

import java.io.File

import com.he.utils.{ConfigurationManager, RedisClient}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 多消费自定义offset
  */
object KafkaDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.INFO)
    Logger.getLogger("org.apache.kafka.clients.consumer").setLevel(Level.INFO)
    val warehouseLocation = new File("hdfs://cluster/hive/warehouse").getAbsolutePath
    val spark = SparkSession.builder().appName("Spark Jason").config("spark.sql.warehouse.dir",    warehouseLocation).enableHiveSupport().getOrCreate()
    spark.conf.set("spark.streaming.concurrentJobs", 10)
    spark.conf.set("spark.streaming.kafka.maxRetries", 50)
    spark.conf.set("spark.streaming.stopGracefullyOnShutdown",true)
    spark.conf.set("spark.streaming.backpressure.enabled",true)
    spark.conf.set("spark.streaming.backpressure.initialRate",5000)
    spark.conf.set("spark.streaming.kafka.maxRatePerPartition", 3000)

    val sc = spark.sparkContext
    val scc = new StreamingContext(sc, Seconds(2))
    val kafkaParams = Map[String, Object](
      "auto.offset.reset" -> "latest",
      "value.deserializer" -> classOf[StringDeserializer]
      , "key.deserializer" -> classOf[StringDeserializer]
      , "bootstrap.servers" -> ConfigurationManager.getProperty("broker")
      , "group.id" -> ConfigurationManager.getProperty("groupId")
      , "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    var stream: InputDStream[ConsumerRecord[String, String]] = null
    val topics = Array("jason_20180519", "jason_0606","jason_test")
    val maxTotal = 200
    val maxIdle = 100
    val minIdle = 10
    val testOnBorrow = false
    val testOnReturn = false
    val maxWaitMillis = 5000
    val jedis = RedisClient.pool.getResource
    jedis.select(ConfigurationManager.getInteger("redis.dbIndex"))
    val keys = jedis.keys(topics(0) + "*")
    val keys_2 = jedis.keys(topics(1) +"*")
    val keys_3 = jedis.keys(topics(2) +"*")
    if(keys.size() == 0 && keys_2.size() == 0 && keys_3.size() == 0){
      println("第一次启动,从头开始消费数据-----------------------------------------------------------")
      stream = KafkaUtils.createDirectStream[String, String](
        scc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      )
    }else{
      println("不是第一次启动,从上次的offest开始消费数据-----------------------------------------------")
      stream = KafkaUtils.createDirectStream[String, String](
        scc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams,Map(new TopicPartition("",1) -> 1L)) //此处使用redis工具类实现
      )
    }
    jedis.close()
    stream.foreachRDD(rdd=>{
      if (!rdd.isEmpty()) {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition(partition=>{
          val o = offsetRanges(TaskContext.get.partitionId)
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
          val jedis_jason = RedisClient.pool.getResource
          jedis_jason.select(ConfigurationManager.getInteger("redis.dbIndex"))
          partition.foreach(pair=>{
            //自己的计算逻辑;
          })
          offsetRanges.foreach { offsetRange =>
            println("partition : " + offsetRange.partition + " fromOffset:  " + offsetRange.fromOffset + " untilOffset: " + offsetRange.untilOffset)
            val topic_partition_key_new = offsetRange.topic + "_" + offsetRange.partition
            jedis_jason.set(topic_partition_key_new, offsetRange.untilOffset + "")
          }
          jedis_jason.close()
        })
      }
    })
    scc.start()
    scc.awaitTermination()
  }
}
