package com.he.mechinelearning.shangxuetang.trafficpredict

import java.text.SimpleDateFormat
import java.util.Date

import com.he.client.RedisPool
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * 根据输入的时间读取各道路前三分钟的速度来预测某道路拥堵情况
  */
object Predict {
  // create the date/time formatters
  val dayFormat = new SimpleDateFormat("yyyyMMdd")
  val minuteFormat = new SimpleDateFormat("HHmm")
  val sdf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss")
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Predict").master("local[*]").getOrCreate()
    val input = "2018-07-14_16:39:00"
    val date = sdf.parse(input)
    val inputTimeLong = date.getTime
    //    val inputTime = new Date(inputTimeLong)
    val day = dayFormat.format(date)//yyyyMMdd

    // fetch data from redis
    val jedis = RedisPool.pool.getResource
    jedis.select(1)

    // find relative road monitors for specified road
    // val camera_ids = List("310999003001","310999003102","310999000106","310999000205","310999007204")
    val camera_ids = List("310999003001", "310999003102")
    val camera_relations: Map[String, Array[String]] = Map[String, Array[String]](
      "310999003001" -> Array("310999003001", "310999003102", "310999000106", "310999000205", "310999007204"),
      "310999003102" -> Array("310999003001", "310999003102", "310999000106", "310999000205", "310999007204"))

    camera_ids.foreach({ camera_id =>
      val list = camera_relations(camera_id)

      val relations = list.map({ camera_id =>
        // fetch records of one camera for three hours ago
        (camera_id, jedis.hgetAll(day + "_'" + camera_id + "'"))
      })

      //      relations.foreach(println)

      // organize above records per minute to train data set format (MLUtils.loadLibSVMFile)
      val aaa = ArrayBuffer[Double]()
      // get current minute and recent two minutes
      for (index <- 3 to (1,-1)) {
        //拿到过去 一分钟，两分钟，过去三分钟的时间戳
        val tempOne = inputTimeLong - 60 * 1000 * index
        val currentOneTime = new Date(tempOne)
        //获取输入时间的 "HHmm"
        val tempMinute = minuteFormat.format(currentOneTime)
        println("inputtime ====="+currentOneTime)
        for ((k, v) <- relations) {
          // k->camera_id ; v->speed
          val map = v
          if (map.containsKey(tempMinute)) {
            val info = map.get(tempMinute).toDouble
//            val f = info
            aaa += info
          } else {
            aaa += -1.0
          }
        }
      }

      // Run training algorithm to build the model
      val path = jedis.hget("model", camera_id)
      if(path!=null){
        val model = LogisticRegressionModel.load(path)
        // Compute raw scores on the test set.
        val prediction = model.predict(Vectors.dense(aaa.toArray))
        println(input + "\t" + camera_id + "\t" + prediction + "\t")
//       jedis.hset(input, camera_id, prediction.toString)
      }
    })

    RedisPool.pool.returnResource(jedis)
  }
}
