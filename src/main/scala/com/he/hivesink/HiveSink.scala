package com.he.hivesink

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * hive动态分区追加写入
 * 写之前合并分区为x个，则最终写到一个分区下的最多文件数为x
 * 针对spark写hive分区下小文件过多的解决办法参考：
 * https://blog.csdn.net/weixin_26752759/article/details/108085207
 */
object HiveSink {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .config("spark.sql.shuffle.partitions", "4")
      .master("local")
      .getOrCreate()

    spark
      .read
      .table("")

      .coalesce(3)
      .write
      .mode(SaveMode.Append)
      .partitionBy("")
      .saveAsTable("")

    spark.close()
  }
}
