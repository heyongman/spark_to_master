package com.he

import java.net.URL

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Test3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("LinearRegressionDemo")
      .master("local[4]")
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

//    105,2007-02-20 00:00:48,121.466600,31.220800,  0, 45,0
    val ds = spark.read.textFile("/Users/he/work/bigdata/docu/db/Taxi_070220/Taxi_105*")
    val car = ds.map(line => {
      val strings = line.split(",")
      assert(strings.length == 7)
      val car_id = strings(0)
      val time = strings(1)
      val longitude = strings(2) //经度
      val latitude = strings(3) //纬度
      val speed = strings(4).trim.toInt
      val direction = strings(5).trim.toInt * 2 //顺时针多少度
      Car(car_id, time, longitude, latitude, speed, direction)
    })
      .orderBy($"time")


    car.show(false)

    val url = new URL("")
    val content = url.getContent


  }
}

case class Car(car_id:String, time:String, longitude:String, latitude:String, speed:Int, direction:Int)
