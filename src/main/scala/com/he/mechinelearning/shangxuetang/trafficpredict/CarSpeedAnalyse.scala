package com.he.mechinelearning.shangxuetang.trafficpredict

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.google.gson.{JsonObject, JsonParser}
import com.he.utils.{ConfigurationManager, RedisClient}
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType


/**
  * 分析kafka每分钟道路的车辆平均速度，存入到redis中
  */
object CarSpeedAnalyse {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CarSpeedAnalyse")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val car: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ConfigurationManager.getProperty("bootstrap.servers"))
      .option("subscribe", ConfigurationManager.getProperty("kafka.topics"))
      .load()

    car.printSchema()

    val record = car.select($"value".cast(StringType)).map(line => {
      val jsonObj = new JsonParser().parse(line.getString(0)).asInstanceOf[JsonObject]
      val camera_id = jsonObj.get("camera_id").getAsString
      val speed = jsonObj.get("speed").getAsInt
      val event_time = jsonObj.get("event_time").getAsString
      val timestamp = new Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(event_time).getTime)
      Car(camera_id, speed, timestamp)
    })
    //    record.select($"camera_id",$"speed",$"event_time".cast(TimestampType))
    record.printSchema()
    val speed = record
      .withWatermark("timestamp", "10 seconds")
      .groupBy(
        window($"timestamp", "60 seconds", "60 seconds"),
        $"cameraId"
      )
      .agg(
        bround(avg($"speed"),2).cast(StringType).alias("avg_speed"),
        first($"timestamp").alias("first_time")
      )
      .select(
        regexp_replace($"cameraId","'","").alias("cameraId"),
        $"avg_speed",
        date_format($"first_time","yyyyMMdd").alias("day"),
        date_format($"first_time","HHmm").alias("minute")
      )


    // 查询
    val query = speed.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

//
    val redisQuery = speed
      .writeStream
      .foreachBatch((batchDF: DataFrame, batchId: Long) => {
      batchDF.foreachPartition(partDF => {
        val jedisPool = RedisClient.pool.getResource
        partDF.foreach(row => {
          val cameraId = row.getString(0)
          val avg_speed = row.getString(1)
          val day = row.getString(2)
          val minute = row.getString(3)
          jedisPool.select(ConfigurationManager.getInteger("redis.db"))
          jedisPool.hset(day + "_" + cameraId, minute, avg_speed)
        })
        RedisClient.pool.returnResource(jedisPool)
      })
    })
      .start()


    query.awaitTermination()
    redisQuery.awaitTermination()
    spark.close()
  }
}

case class Car(cameraId: String, speed: Int, timestamp: Timestamp)