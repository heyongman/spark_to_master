package com.he.mechinelearning.shangxuetang.trafficpredict

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import com.he.utils.{ConfigurationManager, RedisClient}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * 训练回归模型
  */
object TrainLR {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CarSpeedAnalyse")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    val jedisPool = RedisClient.pool.getResource
    jedisPool.select(ConfigurationManager.getInteger("redis.db"))
    val dayFormat = new SimpleDateFormat("yyyyMMdd")
    val minuteFormat = new SimpleDateFormat("HHmm")
    val now = new Date()
    val day = dayFormat.format(now)//yyyyMMdd
    val hours = 5 //根据近五个小时的数据来训练模型

    // 为指定的卡扣找出相邻的卡扣
    val camera_ids = Array("310999003001","310999003102")
    val camera_relations:Map[String,Array[String]] = Map[String,Array[String]](
      "310999003001" -> Array("310999003001","310999003102","310999000106","310999000205","310999007204"),
      "310999003102" -> Array("310999003001","310999003102","310999000106","310999000205","310999007204")
    )

    camera_ids.foreach(camera_id => {
      val relation_ids: Array[String] = camera_relations(camera_id) //相邻的卡扣id
//      找出相邻卡扣的信息，其中也包括自己的
      val relations = relation_ids.map(relation_id => {
        val speed_map: util.Map[String, String] = jedisPool.hgetAll(day + "_" + relation_id)
        (relation_id, speed_map) //(310999003001,{0418=95.33_84}) => (卡扣号,{分钟=速度})
      })

//      relations.foreach(println)

//      循环60*hours分钟,获取期间的数据
      val data_set = ArrayBuffer[LabeledPoint]()
      for (minute <- 0 until 60*hours){
//        构建每一行数据
        var label = -1.0
        val features = ArrayBuffer[Double]() //加括号调用apply方法
        for (index <- 0 to 2){
//          取出往后推移三分钟的数据
          for ((relation_id,speed_map) <- relations){
//            向后推移时间
            val calendar = Calendar.getInstance()
            calendar.add(Calendar.MINUTE,-(minute+index))
            val minute_moved = minuteFormat.format(calendar.getTime)

            if (relation_id == camera_id){
//              获取label值
              label = speed_map.getOrDefault(minute_moved,"-1.0").toDouble
            } else {
//              获取feature值
              features += speed_map.getOrDefault(minute_moved,"-1.0").toDouble
            }
          }
        }
        if (label != -1.0){
          data_set += LabeledPoint(label,Vectors.dense(features.toArray)) //生成LabeledPoint
        }
      }


//      进行模型训练
      val dataSetDF = spark.createDataFrame(data_set)
      dataSetDF.show(false)

      val Array(trainDf,testDF) = dataSetDF.randomSplit(Array(0.8,0.2),1234)
      if (!dataSetDF.isEmpty){
        val lrModel = new LogisticRegression()
          .setMaxIter(100)
          .setFamily("multinomial")
          .setFitIntercept(true)
          .fit(trainDf)

        val summary = lrModel.evaluate(testDF)

        println(summary.accuracy) //准确率
        println(summary.weightedPrecision) //精确率
        println(summary.weightedFMeasure) //f-measure

//        准确率大于0.8就保存该道路的模型
        if(summary.accuracy > 0.8){
          val path = "hdfs://node1:9000/model/model_"+camera_id+"_"+System.currentTimeMillis()
          lrModel.save(path)
          println("saved model to "+ path)
          jedisPool.hset("model", camera_id , path)

        }
      }

    })


    RedisClient.pool.returnResource(jedisPool)
    spark.close()
  }
}
