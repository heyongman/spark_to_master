package com.he.spark2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

object StructuredStreamingDemo {

  case class Line(timestamp:TimestampType,word:String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ssd")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val schema = new StructType()
      .add("record1",StringType)
      .add("record2",StringType)

    val inputs = spark.readStream
//      .schema(schema)
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

//    Kafaka版本>=0.10.0才能使用StructuredStreaming
//    val df = spark
//      .readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
//      .option("subscribe", "topic1")
//      .load()
//
//    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//      .as[(String, String)]
    //    println(inputs.isStreaming)
    //    inputs.printSchema()

    //    dataframe列拆分
    val lines = inputs.withColumn("splitCol", split($"value", ","))
      .select(
        $"splitCol".getItem(0).as("timestamp").cast(TimestampType),
        $"splitCol".getItem(1).as("word")
      )

    lines.printSchema()

    val wc = lines
      .withWatermark("timestamp", "1 day")
      .groupBy(
        window($"timestamp", "1 day", "1 day"),
        $"word"
      ).count()

    // 查询
    val query = wc.writeStream
      .queryName("demo")
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .start()


    query.awaitTermination()
    spark.stop()
  }


}
