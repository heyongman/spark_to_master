package com.he.kafka

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.elasticsearch.spark.sql._

object KafkaOffset {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaIntegration")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .getOrCreate()

    val in1 = read(spark)
    in1.printSchema()

    in1
//      .toJSON
      .writeStream
      .queryName("kafkaIntegration")
      .format("console")
      .option("checkpointLocation", "D://tmp/")
      .option("truncate", "false")
      .option("numRows", 100)
      .outputMode(OutputMode.Append())
//      .start()

    in1
      .writeStream
      .queryName("kafkaOffset")
      .outputMode(OutputMode.Append())
      .foreachBatch((batch,id) => {
//        batch.saveToEs("")
        batch.write
          .format("jdbc")
          .mode(SaveMode.Append)
          .option("url","")
          .option("dbtable","test1")
          .option("user","")
          .option("password","")
          .save()

        saveOffset(batch)
      })
      .start()

    spark.streams.awaitAnyTermination()
    spark.close()
  }

  def saveOffset(batch:DataFrame): Unit ={
    batch.foreachPartition(part => {
      if (part.nonEmpty){
        var partition = 0
        var offset = 0L
        while (part.hasNext){
          val row = part.next()
          partition = row.getAs[Int]("partition")
          offset = row.getAs[Long]("offset")
        }
        println(partition,offset)
      }
    })
  }

  def saveOffset(row:Row): Unit ={
    val partition = row.getAs[Int]("partition")
    val offset = row.getAs[Long]("offset")
    println(partition,offset)
  }


  def read(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val df1: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.33.117:9093")
      .option("subscribe", "test1")
      .option("startingOffsets", "earliest")
      .load()

    val schema1 = new StructType()
      .add("id", StringType)
      .add("createTime", TimestampType)
      .add("orderId", StringType)
    val inputs1 = df1.select($"key".cast(StringType), $"value".cast(StringType), $"topic", $"partition", $"offset")
      .withColumn("value", from_json($"value", schema1))
      .withColumn("createTime", $"value.createTime")
      //      水印不支持struct类型，只能单独拿出来做一列
      .withWatermark("createTime", "5 seconds")

    inputs1
  }

}
