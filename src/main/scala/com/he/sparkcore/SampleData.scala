package com.he.sparkcore

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SampleData {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[4]").setAppName("SampleData")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    import sQLContext.implicits._

    val srcRDD = sc.makeRDD(Array(1,2,3,4,5,6,7,8,9,10,11,12,13))
    val srcRDD1 = sc.makeRDD(Array(1,2,3))
    srcRDD.toDF("")
//    取样
    val sampleRDD: RDD[Int] = srcRDD.sample(false,0.5)
//    交集
    val resRDD = srcRDD.intersection(srcRDD1)
//    差集
    val resRDD1 = srcRDD.subtract(srcRDD1)
//    并集
    val resRDD2 = srcRDD.union(srcRDD1)

    val luckys = srcRDD.takeSample(false,3)


    sc.stop()
  }
}
