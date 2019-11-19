package com.he.mechinelearning.shangxuetang.areaRoadFlow

import java.util.Properties

import com.he.utils.{ConfigurationManager, Constants}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * 计算出每一个区域top3的道路流量
  * 每一个区域车流量最多的3条道路
  * 计算道路下各个卡扣的流量，每条道路有多个卡扣（monitor_id）
  */
object AreaTop3RoadFlowAnalyze {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AreaTop3RoadFlowAnalyze")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions","4")
      .enableHiveSupport()
      .getOrCreate()

//    获取指定日期范围内的监控数据
    getMonitorDataByDate(spark,"2018-04-25","2018-04-27")
//    获取mysql中区域对应信息表
    getAreaDataFromMysql(spark)
//    将监控数据与mysql区域对应表关联
    integrateMonitorDataWithArea(spark)
//    求取每个区域Top3的道路流量
    getRoadFlowTop3ByArea(spark)

    spark.close()
  }

  /**
    * 获取指定日期范围内的监控数据
    */
  def getMonitorDataByDate(spark: SparkSession, startTime: String, endTime: String): Unit = {
    val sql = s"select date,monitor_id,camera_id,car,action_time,speed,road_id,area_id from traffic.monitor_flow_action where date between '$startTime' and '$endTime'"
    spark.sql(sql).createOrReplaceTempView("MonitorDataByDate")
  }

  /**
    * 获取mysql中区域对应信息表
    */
  def getAreaDataFromMysql(spark: SparkSession): Unit = {
    val url = ConfigurationManager.getProperty(Constants.JDBC_URL)
    val table = "area_info"
    val properties = new Properties()
    properties.setProperty("user",ConfigurationManager.getProperty(Constants.JDBC_USER))
    properties.setProperty("password",ConfigurationManager.getProperty(Constants.JDBC_PASSWORD))

    spark.read.jdbc(url,table,properties).createOrReplaceTempView("AreaDataFromMysql")
  }

  /**
    * 将监控数据与mysql区域对应表关联
    */
  def integrateMonitorDataWithArea(spark: SparkSession): Unit = {
    import spark.implicits._
    val sql = "select date,monitor_id,camera_id,car,action_time,speed,road_id,a.area_id,area_name from AreaDataFromMysql a left join MonitorDataByDate b on a.area_id = b.area_id"
    //    根据area_name和road_id分组时可能产生数据倾斜，采用双次聚合解决
    //    1.加前缀聚合：1_东城区 23 / 3_东城区 56
    //    2.去前缀聚合：东城区 79
    spark.sql(sql)
      .withColumn("prefix_area_name_road_id",concat_ws("_",floor(rand()*10),$"area_name",$"road_id"))
      .select($"area_name",$"road_id",$"monitor_id",$"car",$"prefix_area_name_road_id")
      .groupBy($"prefix_area_name_road_id",$"monitor_id")
      .agg(count($"car").as("flow_monitor"))
      .withColumn("area_name_road_id",substring($"prefix_area_name_road_id",3,20))
      .groupBy($"area_name_road_id",$"monitor_id")
      .agg(sum($"flow_monitor").as("flow_monitor"))
      .groupBy($"area_name_road_id")
      .agg(
        collect_list(concat_ws("=",$"monitor_id",$"flow_monitor")).as("flow_monitor"),
        sum($"flow_monitor").as("flow_road")
      )
      .withColumn("splitColumn",split($"area_name_road_id","_"))
      .select(
        $"splitColumn".getItem(0).as("area_name"),
        $"splitColumn".getItem(1).as("road_id"),
        $"flow_monitor",
        $"flow_road"
      ).createOrReplaceTempView("MonitorDataWithArea")

  }

  /**
    * 求取每个区域Top3的道路流量
    */
  def getRoadFlowTop3ByArea(spark: SparkSession): Unit = {
    val roadFlowTop3ByAreaSql = "select area_name,road_id,flow_road,case when flow_road>=190 then 'A' when flow_road>=180 then 'B' when flow_road>=170 then 'C' else 'D' end flow_level,flow_monitor from " +
      "(select area_name,road_id,flow_road,flow_monitor,row_number() over(partition by area_name order by flow_road desc) as flow_rank from MonitorDataWithArea having flow_rank <=3) as a "
    val resDF = spark.sql(roadFlowTop3ByAreaSql)
    resDF.write.saveAsTable("traffic_res.flow_road_area_top3")
  }

}
