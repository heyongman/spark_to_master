package com.he.kafka

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaBatchQuery {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaIntegration")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.24.105.144:9093")
      .option("subscribe", "test1")
      .option("startingOffsets","earliest")
//      .option("","")
      .load()

    val schema1 = new StructType()
      .add("id", StringType)
      .add("createTime", TimestampType)
      .add("orderId", StringType)

    val frame = df.selectExpr("cast(value as string)")
      .withColumn("value", from_json($"value", schema1))
    //      .withColumn("value_createTime",$"value.createTime")


    frame.printSchema()
    frame.show(false)

    spark.close()
  }

}
