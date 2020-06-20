package com.he.spark2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object MyUDF {

  /**
    * 一个udf
    */
  val fun1: UserDefinedFunction = udf((x1: String, x2: String) => {
    x1 == x2
  })

  /**
    * 全角转半角
    */
  val toDBC: UserDefinedFunction = udf((x: String) => {
    val c: Array[Char] = x.toCharArray
    for (i <- c.indices) {
      if (c(i) > 65280 && c(i) < 65375) {
//       其他字符半角(33-126)与全角(65281-65374)的对应关系是：均相差65248
        c(i) = (c(i) - 65248).toChar
      } else if (c(i) == 12288) {
//       全角空格为12288，半角空格为32
        c(i) = 32.toChar
      }
    }
    new String(c)
  })

  def main(args: Array[String]): Unit = {
//    udf测试
    val spark = SparkSession.builder().appName("test10").master("local[*]").getOrCreate()
    import spark.implicits._
//    spark.sql("select *,toDBC(col) as col1 from table")
    val frame = Seq("1", "2", "１２　３安抚qaQAＱＡ").toDF("col")
    frame
      .withColumn("col1",toDBC($"col"))
      .show()

    spark.close()
  }
}
