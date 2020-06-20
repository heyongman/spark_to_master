package com.he

import java.util
import java.util.concurrent.{CountDownLatch, Executors}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, MapType, StructField, StructType}

object Test10 {
  import org.apache.spark.sql.functions._

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test10").master("local[*]").getOrCreate()
    import spark.implicits._
    val frame = (1 to 100)
      .toDF("a")
      .repartition(5)
      .withColumn("b", lit("sfs"))
      .withColumn("c", spark_partition_id())
      .withColumn("d", monotonically_increasing_id())
      .withColumn("e", struct($"a", $"b"))

    frame.printSchema()
    frame.show(100)

//    new CountDownLatch(1).await()
    spark.close()
  }
}
