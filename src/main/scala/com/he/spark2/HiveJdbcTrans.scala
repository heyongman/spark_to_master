package com.he.spark2

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark2Demo {
  def main(args: Array[String]): Unit = {
    //    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("Spark2Demo")
      .master("local[4]")
      .enableHiveSupport()
      //      .config("spark.sql.warehouse.dir", "")
      .getOrCreate()


    import spark.implicits._

    //    spark-sql
    //    val srcDF: DataFrame = spark.read.text("/Users/he/work/maiyue/项目/桂电/REC_BANKREC.txt")
    //    val sampleDS: Dataset[Row] = srcDF.sample(false,0.59938)
    //    sampleDS.coalesce(1).write.text("/Users/he/work/maiyue/项目/桂电/REC_BANKREC-100M.txt")

    //    val srcDF = spark.read
    //      .option("header", "true")
    //      .option("delimiter", ";")
    //      .csv("/Users/he/work/maiyue/项目/桂电/REC_BANKREC-1K.txt")

    //    srcDF.show()
    //        srcDF.printSchema()
    //    val df = srcDF.select($"BANKRECNO", ($"CUSTOMERID" + 1).alias("CUSTOMERID").cast("Double"), $"OUTID")
    //      .createOrReplaceTempView("REC_BANKREC")

    //    df.printSchema()
    //    df.filter($"CUSTOMERID" > 100000).show()
    //    df.groupBy("BANKRECNO").count().show()

    //    val DS_RB = spark.read.json("/Users/he/work/maiyue/项目/桂电/people.json")
    //    DS_RB.show()
    //    DS_RB.printSchema()
    //    spark.sql("select * from default.tb_test").show()
    //    spark.sql("create database zsy")
    //    spark.sql("show tables").show()
    //    spark.sql("create table if not exists zsy.tb_zsy(name string,value string)").show()
    //    spark.sql("set hive.support.concurrency=true").show()
    //    spark.sql("set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager").show()
    //    spark.sql("select * from weblogs").show()

//    使用jdbc方式读取hive事务表数据，用spark-sql不支持
    val HiveDialect = new JdbcDialect {
      override def canHandle(url: String): Boolean = url.startsWith("jdbc:hive2") || url.contains("hive2")
      override def quoteIdentifier(colName: String): String = {
        s"$colName"
      }
    }
    JdbcDialects.registerDialect(HiveDialect)

    val url = "jdbc:hive2://localhost:10000/default?hive.support.concurrency=true;hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager"
    val myTable = "flume_user"
    val dbProperties = new Properties()
    dbProperties.setProperty("driver", "org.apache.hive.jdbc.HiveDriver")
    dbProperties.setProperty("user", "root")
//    dbProperties.setProperty("hive.support.concurrency","true")
//    dbProperties.setProperty("hive.txn.manager","org.apache.hadoop.hive.ql.lockmgr.DbTxnManager")
    val df = spark.read.jdbc(url, myTable, dbProperties)
    df.show()


    spark.stop()
  }
}

case class REC_BANKREC(name: String, age: Int)
