package com.he.mechinelearning.shangxuetang.areaRoadFlow

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer


object Monitor1StepConvertRateAnalyze {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("Monitor1StepConvertRateAnalyze")
      .config("spark.sql.shuffle.partitions", "4")
      .enableHiveSupport()
      .getOrCreate()

    //    指定的卡扣流
    val bd: Broadcast[String] = spark.sparkContext.broadcast("0001,0002,0003,0004,0005")

    val dateRangeDF = getMonitorDataByDate(spark,"2018-04-25","2018-04-27")

    val carInfoRDD = getCarInfoRDD(spark,dateRangeDF) //获取通过卡口的 车牌，时间，卡口号

    val monitorStreamCountRDD = computeMatchMonitorStream(carInfoRDD,bd) //根据指定卡口流计算匹配次数

    val arrayBuffer = computeRoadConvertRate(monitorStreamCountRDD) //计算转化率

    arrayBuffer.foreach(println)


    spark.close()
  }

  /**
    * 获取指定日期范围内的监控数据
    */
  def getMonitorDataByDate(spark: SparkSession, startTime: String, endTime: String) = {
    val sql = s"select date,monitor_id,camera_id,car,action_time,speed,road_id,area_id from traffic.monitor_flow_action where date between '$startTime' and '$endTime'"
    spark.sql(sql)
  }

  /**
    * 获取通过卡口的 车牌，时间，卡口号
    * @param spark
    * @param df
    * @return
    */
  def getCarInfoRDD(spark: SparkSession,df:DataFrame) ={
//    车牌，时间，卡口号  (深W61995,2018-04-26 02:03:42,0007)
    df.rdd.map(x => (x.getString(3),(x.getString(4),x.getString(1))))
  }

  /**
    * 根据指定的卡口流分片计算车辆匹配的次数
    * @param rdd
    * @param bd 指定的卡口流
    */
  def computeMatchMonitorStream(rdd: RDD[(String,(String,String))],bd: Broadcast[String]) ={
    val monitorStream: Array[String] = bd.value.split(",")

    val groupedCarOrderByTime = rdd.groupByKey().flatMap(line => {
      val locus = line._2
      val sortedMonitors = locus
        .toSeq.sortWith(_._1 < _._1) //根据时间排序，获取轨迹
        .map(_._2) //仅获取所有monitor

      val carLocus = sortedMonitors.mkString(",")
      val carLocusLen = carLocus.length
      val len = bd.value.length
      var resArray = ArrayBuffer.empty[(String,Int)]

      /**
        * 子流     /计数 [counts]
        * 1       /453
        * 1,2     /44
        * 1,2,3   /25
        * 1,2,3,4 /12
        */
      for (x <- 1 to len){
        val flowSlice = monitorStream.take(x).mkString(",")
//        计算子流出现的次数
        val counts = (carLocusLen-carLocus.replaceAll(flowSlice,"").length)/x
        resArray += ((flowSlice,counts))
      }
      resArray
    })
    groupedCarOrderByTime
  }

  def computeRoadConvertRate(rdd: RDD[(String,Int)]) ={
    val tuples = rdd.reduceByKey(_+_).sortBy(_._1.length)
    val arrayBuffer = ArrayBuffer.empty[(String,Double)]
//    使用reduceLeft计算转化率
    tuples.collect().reduceLeft((v1,v2) =>{
      arrayBuffer += ((v2._1,v2._2.toDouble/v1._2))
      v2
    })
    arrayBuffer
  }

}
