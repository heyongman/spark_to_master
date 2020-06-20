package com.he

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object Test6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test6").master("local[1]").getOrCreate()
    val rdd = spark.sparkContext.makeRDD(Array(1,2,3,4,5))
    val value = rdd.flatMap(l => {
      val ints = new ArrayBuffer[String]()
      for (i <- Range(1, 6)) {
        ints += i + "_" + l
      }
      ints
    })


    value.foreach(println)
  }


}
