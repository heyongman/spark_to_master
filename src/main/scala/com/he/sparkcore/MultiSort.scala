package com.he.sparkcore

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Sorting

/**
  * 多种排序方式
  */
object MultiSort {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("SecondarySort").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val srcRDD = sc.makeRDD(Array("dd 17 10","de 17 30","da 15 28","db 14 28","dc 17 28","da 14 28"))
    val line: RDD[(String, Int, Int)] = srcRDD.map(l => {
      val s = l.split(" ")
      (s(0), s(1).toInt, s(2).toInt)
    })
    srcRDD.foreach(println)
    println("排序后：")

//    sortBy多字段排序
    line.sortBy(l => (l._3,l._2,l._1),false).foreach(println) //单个字段排序直接传参数，多个传tuple

//    sortByKey根据key排序
    line.map(l => (l._3,l._2)).sortByKey(false).foreach(println)


//    Ordering
    val pairs = Array(("a", 5, 2), ("c", 3, 1), ("b", 1, 3))
    // sort by 2nd element
    Sorting.quickSort(pairs)(Ordering.by[(String, Int, Int), Int](_._2))
    // sort by the 3rd element, then 1st
    Sorting.quickSort(pairs)(Ordering[(Int, Int, String)].on(x => (x._3, x._2, x._1)))
    println(pairs.toList)

//    top单个字段排序
    line.top(6)(Ordering.by[(String, Int, Int), String](_._1)).foreach(println)
//    top多个字段排序
    line.top(6)(Ordering[(Int, Int, String)].on(x => (x._3, x._2, x._1))).foreach(println)

    sc.makeRDD(Array(1,2,3,4)).sortBy(x => x,false).foreach(println)

    Array(1,2,3,4).sortWith(_>_).foreach(println)

    def double(x: Int): Int = x*2
    var x : (Int) => Int = double

    sc.stop()
  }
}
