package com.he.uds

import org.apache.spark.sql.SparkSession
import com.he.uds.MysqlUpdate._
import com.he.utils.Config

object Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaIntegration")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()
    import spark.implicits._

    val data = (1 to 100).map(x => (x, x % 10, x + "sfcds")).toDF("col1,col2,col3")
    data.updateToDB("",Config.mysqlLocal)

    spark.close()
  }

}
