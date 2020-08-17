package com.he

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, DoubleType}

object Test13 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test10").master("local[*]").getOrCreate()
    import spark.implicits._

    val strings = Seq("", "129.123123",null)
    val strings1 = Seq("", "129.123122",null)
    val frame = strings.toDF("a").alias("a")
    var frame1 = strings.toDF("a").alias("b")

//    <=> 两者为null返回相等，===两者为null返回不相等，在inner join时结果明显，但一般为null的值关联时无需保留
    frame.join(frame1,$"a.a" === $"b.a","inner")
        .show()


    spark.close()
  }

}
