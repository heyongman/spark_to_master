package com.he.sparkcore

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupTopN {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[1]").setAppName("GroupTopN")
    val sc = new SparkContext(conf)
    val score = sc.textFile(this.getClass.getClassLoader.getResource("./score.txt").getPath)

    val line = score.map(l => {
      val s = l.split(" ")
      (s(0), s(1))
    })
    val group: RDD[(String, Iterable[String])] = line.groupByKey()
    val sortedGroup = group.map(l => {
      var a = l._1
      var b = l._2

      val sorted = b.toSeq.sortWith(_>_).sortBy(x => (x.size,x.length))
      (a, sorted)
    })
    sortedGroup.foreach(println)

  }

}
