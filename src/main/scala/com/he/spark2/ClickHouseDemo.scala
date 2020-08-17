package com.he.spark2


import java.util.concurrent.CountDownLatch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ClickHouseDemo {
  def main(args: Array[String]): Unit = {
    //    ru.yandex.clickhouse.ClickHouseDriver
    val spark = SparkSession.builder()
      .appName("ClickHouseDemo")
      .master("local[4]")
      .getOrCreate()

    val frame = spark.read.format("jdbc")
      .option("url", "jdbc:clickhouse://localhost:8123/default")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("dbtable", "default.gc")
//      .option("query", "select deviceNo,count(1) as count from default.gc group by deviceNo") //query
      .option("user", "default")
      .option("password", "123")
      .load()

    import spark.implicits._
    frame
//      .groupBy($"deviceNo")
//      .agg(count($"deviceNo") as "count")
      .show()

    new CountDownLatch(1).await()
    spark.close()
  }

}
