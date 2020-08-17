package com.he

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.streaming.StreamingContext

object Test12 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test10").master("local[*]").getOrCreate()
    import spark.implicits._

    val strings = Array(("1",1), ("",2), ("2",3), (null,4),("-",5))

//    =!= "" 可以过滤null和空字符串
    val value = spark.createDataset(strings)
    value.filter($"_1".isNull || $"_1" === "")
      .show()

    value.filter($"_1" =!= "" && $"_2" =!= 3)
      .show()

    value
//      .na.drop("all",Seq("_1"))
      .withColumn("_11",$"_1".cast(IntegerType)) //转换失败的是null
      .withColumn("isnan",isnan($"_1"))
//      .dropDuplicates("_1")
      .show()

//    val accessInfo = spark.read.option("header", value = true).csv("D:\\文档\\项目\\贵阳TOCC\\数据采集接口\\交管局\\卡口基本信息新2.csv")
//        .select($"DEVICE_NO",$"DEVICE_LOCATION",$"DIRECTION",$"DEVICE_STATUS",$"LONGITUDE",$"LATITUDE")
//    accessInfo
//      .filter(! $"LONGITUDE" =!= "")
//      .show()

    spark.close()
  }

}
