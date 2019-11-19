package com.he.mechinelearning.shangxuetang.areaRoadFlow

import org.apache.spark.sql.SparkSession

object Test1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AreaTop3RoadFlowAnalyze")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions","4")
      .enableHiveSupport()
      .getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._
//    spark.sql("use traffic_res")
//    spark.sql("show tables").show()traffic_res.roadflowtop3byarea
//    spark.sql("select * from traffic_res.roadflowtop3byarea").show()
    val ds = spark.range(0,10)
    ds.withColumn("val",concat_ws("_",$"id",floor(rand()*1000000)))
      .withColumn("test1",split($"val","\\d_").getItem(1))
      .withColumn("test2",substring($"val",-3,5))
      .show()

//    ds.withColumn("val",Random.nextInt(10)).show()
  }
}
