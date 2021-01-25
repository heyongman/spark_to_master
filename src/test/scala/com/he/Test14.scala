package com.he

import com.he.utils.Config
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

object Test14 {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val stringToString = collection.mutable.Map(Config.es.toSeq: _*) += ("es.nodes" -> "hadoop01:9092","key" -> "value")
    println(Config.es,stringToString)

  }

}
