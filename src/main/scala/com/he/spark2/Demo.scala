package com.he.spark2

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object Demo {


  def main(args: Array[String]): Unit = {
    val test = new Test
    test.value
    test.value_=(1)


    val spark = SparkSession.builder().appName("Demo").master("local[2]").getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
//    spark2.+对dataframe map需要隐式转换
    implicit val encoder: Encoder[Row] = org.apache.spark.sql.Encoders.kryo[Row]


    //    val textDS = spark.read.textFile(this.getClass.getClassLoader.getResource("./score.txt").getPath)
    val ds = spark.createDataset(Array((2,"1-3"),(1,"1-3"),(2,"2-5"),(1,"2-5"),(2,"2-6"),(1,"2-6"),(2,"1-6"),(1,"1-6")))

    val df = spark.createDataFrame(Array((2,"1-3"),(1,"1-3"),(2,"2-5"),(1,"2-5"),(2,"2-6"),(1,"2-6"),(2,"1-6"),(1,"1-6"))).toDF("type","weeks")
    val value: Dataset[Row] = df.map(it => {
      it
    })
    ds.rdd.toDF
//      .map(it => it)
      .show()

//    value.foreach(println(_))

    val function = udf({ (ty: Int, weeks: String) =>
      val weeks_str = weeks.split("-").map(_.toInt)
      val week_arr = ArrayBuffer.empty[Int]
      for (week <- weeks_str(0) to weeks_str(1)) {
        if (ty == 2) {
          if (week % 2 == 0) {
            week_arr += week
          }
        } else {
          if (week % 2 != 0) {
            week_arr += week
          }
        }
      }
      week_arr
    })


//    df.withColumn("week_list",function($"type",$"weeks")).show()



    spark.stop()
  }
}
