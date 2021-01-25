package com.he.spark2

import org.apache.spark.{Success, TaskFailedReason}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SaveMode, SparkSession}


class ListenerDemo extends SparkListener{
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    super.onApplicationStart(applicationStart)
    val id = applicationStart.appId.getOrElse("unknown")
    println("appId:",id)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    super.onApplicationEnd(applicationEnd)
    val time = applicationEnd.time
    println("endTime:",time)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    super.onStageCompleted(stageCompleted)
    val written = stageCompleted.stageInfo.taskMetrics.outputMetrics.recordsWritten
    val reason = stageCompleted.stageInfo.failureReason
    println("stageWritten:",written,reason.getOrElse(""))
  }

}

object ListenerDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.extraListeners",classOf[ListenerDemo].getName)
      .getOrCreate()
    import spark.implicits._

    val inputs: DataFrame = (1 to 10).toDF("value")
//    implicit val encoder: Encoder[String] = org.apache.spark.sql.Encoders.kryo[String]
    implicit val encoder: Encoder[Row] = org.apache.spark.sql.Encoders.kryo[Row]

    inputs
      .withColumn("value",$"value".cast(StringType))
//      .map(row => {
//        if ("5".equals(row.getString(0))){
//          throw new NullPointerException("test exception")
//        }
//        row.getString(0)
//      })
      .write.text("D://tmp/test")

    spark.close()
  }


}
