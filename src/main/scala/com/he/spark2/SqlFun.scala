package com.he.spark2


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.he.spark2.MyUDF.{toDBC,fun1}

object SqlFun {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test10").master("local[*]").getOrCreate()
    import spark.implicits._

    val frame = (1 to 100)
      .toDF("a")

      .repartition(5)
      .withColumn("b", lit("a,b"))
      .withColumn("c", spark_partition_id())
      .withColumn("d", monotonically_increasing_id())
      .withColumn("e", typedLit(Seq(1,2,3)))
//      case when
      .withColumn("f",
        when($"a" <= 10,"a1")
        .when($"a" <= 50,"a2")
        .otherwise("other"))
//      行转列
      .withColumn("g", split($"b",","))
      .filter(size($"g") === 2)
      .withColumn("h",$"g"(0))
      .withColumn("i",$"g"(1))
//      解析字段中的json
      .withColumn("j",lit("{\"k1\":1,\"k2\":\"kk\"}"))
      .withColumn("k",from_json($"j",
        StructType(
          Seq(StructField("k1",IntegerType,nullable = false),
            StructField("k2",StringType,nullable = false))
        )))
      .withColumn("l",$"k".getField("k1"))
      .withColumn("m",$"k".getField("k2"))
//      窗口函数
      .withColumn("n",row_number() over Window.partitionBy($"f").orderBy($"a"))
      .withColumn("o",fun1($"l",$"n"))

//    构造schema
    val schema = new StructType()
      .add("_c0",IntegerType,nullable = true)
      .add("carat",DoubleType,nullable = true)

//    排序
//    val value = frame.sort(asc("a"), desc("b"))

    frame.printSchema()
    frame.show(100)


    //    日期处理
    val date = Seq(("2020-06-20","2020-06-29 01:02:03")).toDF("date","datetime")
    val frame1 = date
      //      .where($"" <= "" && $"" >= "")
      .withColumn("curr_date", current_date())
      .withColumn("current_timestamp", current_timestamp())
      .withColumn("unix_timestamp", unix_timestamp())
      .withColumn("add_month", add_months($"date", 2))
      .withColumn("add_day", date_add($"datetime", 2))
      .withColumn("sub_day", date_sub($"datetime", 2))
      .withColumn("date_format", date_format($"datetime", "yyyy-MM"))
      .withColumn("date_diff", datediff($"date", $"datetime"))
      .withColumn("months_between", months_between($"date", $"datetime"))
      .withColumn("next_day", next_day($"date", "monday"))
      .withColumn("from_unixtime", from_unixtime($"unix_timestamp"))
      .withColumn("date_trunc", date_trunc("month", $"datetime"))
      .withColumn("date_trunc1", date_trunc("month", $"date"))
      .withColumn("trunc", trunc($"date", "month"))
      .withColumn("trunc1", trunc($"datetime", "month"))

    frame1.printSchema()
    frame1.show(false)


    spark.close()
  }

}
