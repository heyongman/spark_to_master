package com.he.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 笛卡尔乘积，用于搭配或组合
  */
object Cartesian {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Cartesian").setMaster("local")
    val sc = new SparkContext(conf)

    val nums = sc.makeRDD(1 to 5)
    val chars = sc.makeRDD(('a' to 'c').toList)
    val carte: RDD[(Int, Char)] = nums.cartesian(chars)
    carte.foreach(println)

    sc.stop()
  }
}
