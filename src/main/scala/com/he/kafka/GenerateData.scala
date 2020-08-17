package com.he.kafka

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import collection.JavaConversions._

object GenerateData {
  private val delaySeconds = 5
  private val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers","172.30.163.98:9093")
    properties.put("key.serializer",classOf[StringSerializer])
    properties.put("value.serializer",classOf[StringSerializer])
    val producer = new KafkaProducer[String,String](properties)
    val threads = new util.ArrayList[Thread]()
    println(s"**${format.format(new Date())}**")
    for (i <- 1 to 1000){
      if (i % 100 == 0){
        val thread = new Thread(new Runnable {
          override def run(): Unit = {
            val createTime = format.format(new Date())
            toTest2(producer, i,createTime)
            Thread.sleep(delaySeconds * 1000)
            println("Delay data-> createTime:"+createTime+",sendTime:"+format.format(new Date()))
            toTest1(producer, i,createTime)
          }
        })
        threads.add(thread)
        thread.start()
      } else {
        val createTime = format.format(new Date())
        toTest1(producer,i,createTime)
        toTest2(producer,i,createTime)
        Thread.sleep(100)
      }
    }

    for (t <- threads){
      t.join()
    }
    producer.close()
    println(s"**${format.format(new Date())}**")
  }


  def toTest1(producer: KafkaProducer[String, String],c:Int,time:String): Unit = {
    val record = new ProducerRecord[String,String]("test1", s"""{"id":"$c","createTime":"$time","orderId":"$c"}""")
    producer.send(record)
  }

  def toTest2(producer: KafkaProducer[String, String],c:Int,time:String): Unit = {
    val orderType = c % 3 match {
      case 0 => "a"
      case 1 => "b"
      case 2 => "c"
    }

    val record = new ProducerRecord[String,String]("test2", s"""{"id":"$c","createTime":"$time","orderType":"$orderType","orderDetail":"detail-$c"}""")
    producer.send(record)
  }

}
