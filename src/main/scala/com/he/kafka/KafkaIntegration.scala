package com.he.kafka

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{DateType, LongType, StringType, StructType, TimestampType}

/**
 * 和GenerateData一起做水印的验证
 * Structured Streaming是完全基于事件驱动的，所以数据会延迟水印+窗口事件到来，
 * 测试时可以生成1000条数据，过了延迟时间再生成1条数据，最后就会把前1000条数据的结果全部输出
 *
 * 测试数据中是每100条test1延后5秒，水印设置的延迟也是5秒，最终能把迟到的数据全部关联起来
 *
 * 通过join的expr设置状态的过期规则，spark的checkpoint目录的状态文件也被清理完成
 */
//noinspection DuplicatedCode
object KafkaIntegration {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaIntegration")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .getOrCreate()

    val (in1,in2) = read(spark)
    val out = compute(spark, in1, in2)
    query(spark,out)

    spark.close()
  }

  def read(spark: SparkSession): (DataFrame, DataFrame) ={
    import spark.implicits._
    val df1: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.24.101.167:9093")
      .option("subscribe", "test1")
      .option("startingOffsets","earliest")
      .load()

    val df2: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.24.101.167:9093")
      .option("subscribe", "test2")
      .option("startingOffsets","earliest")
      .load()

    val schema1 = new StructType()
      .add("id", StringType)
      .add("createTime", TimestampType)
      .add("orderId", StringType)
    val inputs1 = df1.select($"key".cast(StringType), $"value".cast(StringType), $"topic", $"partition", $"offset")
      .withColumn("v1",from_json($"value",schema1))
      .withColumn("v1_createTime",$"v1.createTime")
//      水印不支持struct类型，只能单独拿出来做一列
      .withWatermark("v1_createTime","5 seconds")

    val schema2 = new StructType()
      .add("id", StringType)
      .add("createTime", TimestampType)
      .add("orderType", StringType)
      .add("orderDetail", StringType)
    val inputs2 = df2.select($"key".cast(StringType), $"value".cast(StringType), $"topic", $"partition", $"offset")
      .withColumn("v2",from_json($"value",schema2))
      .withColumn("v2_createTime",$"v2.createTime")
      .withWatermark("v2_createTime","5 seconds")

    (inputs1,inputs2)

  }

  def compute(spark: SparkSession, inputs1: DataFrame, inputs2: DataFrame): DataFrame ={
    import spark.implicits._

    inputs1.printSchema()
    inputs2.printSchema()

    val writeStream = inputs1.join(
      inputs2,
      expr(
        """
        v1.id = v2.id and
        v1_createTime >= v2_createTime - interval 1 minute and
        v1_createTime <= v2_createTime + interval 1 minute
        """),
      "left"
    )
      .groupBy(
        window($"v2_createTime","10 seconds","10 seconds"),
        $"v2.orderType"
      )
      .agg(count($"v2.orderDetail") as "count")

    writeStream
  }

  def query(spark: SparkSession,writeStream:DataFrame): Unit ={
    import spark.implicits._

    writeStream.printSchema()

    val query = writeStream
      .toJSON
      .writeStream
      //      指定queryName才可以从故障中恢复
      .queryName("kafkaIntegration")
      .format("console")
      .option("kafka.bootstrap.servers", "172.24.101.167:9093")
      .option("topic","result")
      .option("checkpointLocation", "D://tmp/")
      .option("truncate", "false")
      .option("numRows", 100)
      .outputMode(OutputMode.Append())
//      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }

}
