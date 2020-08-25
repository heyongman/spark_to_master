package com.he.spark2

import java.util.concurrent.CountDownLatch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg

object ParallelTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark2Demo")
      .config("spark.sql.shuffle.partitions", "3")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    val dept = spark.read.json("D:\\proj-20190610\\proj\\bigdata_proj\\spark_to_master\\src\\main\\resources\\department.json")
    val employee = spark.read.json("D:\\proj-20190610\\proj\\bigdata_proj\\spark_to_master\\src\\main\\resources\\employee.json")

    val df = employee
      .filter($"age" > 20)
      .join(dept, $"id" === $"depId")
      .groupBy(dept("name"), employee("gender"))
      .agg(avg(employee("salary")), avg(employee("age")))

    df.show()
    new CountDownLatch(1).await()
  }
}
