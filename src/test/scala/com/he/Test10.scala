package com.he

import java.util
import java.util.concurrent.{CountDownLatch, Executors}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, MapType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

object Test10 {
  import org.apache.spark.sql.functions._

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test10").master("local[*]").getOrCreate()
    import spark.implicits._
//    unix的时间戳要用from_unix_timestamp
    val frame = spark.sql("select cast('2020-06-28 18:18:19' as timestamp) as timestamp,'abc' as name")
      .withColumn("struct",struct($"timestamp",$"name"))
      .toJSON
//      .withColumn("value",to_json(struct($"timestamp")))

    frame.printSchema()
    frame.show(false)
//    val frame = (1 to 100)
//      .toDF("a")
//      .repartition(5)
//      .withColumn("b", lit("sfs"))
//      .withColumn("c", spark_partition_id())
//      .withColumn("d", monotonically_increasing_id())
//      .withColumn("e", struct($"a", $"b"))

//    frame.printSchema()
//    frame.show(100)
//    frame.persist(StorageLevel.MEMORY_ONLY)
//    frame.write.text("D://tmp/test1")
//    frame.write.csv("D://tmp/test2")
//    frame.unpersist()

//    new CountDownLatch(1).await()
    spark.close()
  }
}
