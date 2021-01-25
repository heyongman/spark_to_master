package com.he.spark2

import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object LazyVarUsing {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    val inputs: DataFrame = (1 to 10).toDF("value")
    val pool: LazyVarTest = LazyVarTest()

    inputs
      .repartition(2)
      .foreach(part => {
        val partId = TaskContext.getPartitionId()
        pool.using(pool => {
          println(partId,pool.hashCode())
        })
        Thread.sleep(1000)
      })

    spark.close()
  }


}
