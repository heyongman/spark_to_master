package com.he.mechinelearning.shangxuetang.trafficpredict

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.he.utils.ConfigurationManager
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.json.JSONObject

object KafkaEventProducer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("KafkaEventProducer").master("local[*]").getOrCreate()
    import spark.implicits._

    val topic = ConfigurationManager.getProperty("kafka.topics")
    val props = new Properties()
    props.put("bootstrap.servers", ConfigurationManager.getProperty("bootstrap.servers"))
    props.put("acks", "all")
    //    props.put("retries", 0)
    //    props.put("batch.size", 16384)
    //    props.put("linger.ms", 1)
    //    props.put("buffer.memory", 33554432)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val car = spark.read.textFile("/Users/he/work/bigdata/media/机器学习与数据挖掘/02-回归算法/代码/TrafficPredict/data/2014082013_all_column_test.txt")
    val records = car.filter(_.startsWith("'")).map(_.split(",")).collect()

    val producer = new KafkaProducer[String, String](props)
//    循环生产数据
    for (i <- 1 to 200){
      println(i)
      for (r <- records) {

        val event = new JSONObject()
        event.put("camera_id", r(0))
          .put("car_id", r(2))
          .put("event_time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))
          .put("speed", r(6))
          .put("road_id", r(13))

        val record = new ProducerRecord(topic, r(0), event.toString)
        producer.send(record)
      }
      Thread.sleep(10000)
    }

    producer.close()
    spark.close()

  }
}
