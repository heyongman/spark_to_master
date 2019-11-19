package com.he.sparkcore

import org.apache.spark.util.DoubleAccumulator
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

object AccumulatorVar {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("AccumulatorVar")
    val sc = new SparkContext(conf)
//      定义
    val sum: Accumulator[Int] = sc.accumulator(0) //1.x
    val doubleAcc: DoubleAccumulator = sc.doubleAccumulator("doubleAcc") //2.x

    val srcRDD = sc.makeRDD(Array(1,2,3,4))
    srcRDD.foreach(num => {
//      累加
      sum += num
      doubleAcc.add(num)
    })
//      获取值
    println(sum.value)
    println(doubleAcc.value)



  }
}
