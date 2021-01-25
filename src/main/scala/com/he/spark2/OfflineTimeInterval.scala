package com.he.spark2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OfflineTimeInterval {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark2Demo")
      .master("local[4]")
      .getOrCreate()
    import spark.implicits._

    val source = Seq((1, "2020-09-21 09:30:00"), (2, "2020-09-21 09:33:00"), (3, "2020-09-21 09:36:00"), (4, "2020-09-21 09:38:00"))
      .toDF("id", "time")

    source
        .withColumn("min",substring($"time",15,2))

    spark.close()
  }
}
