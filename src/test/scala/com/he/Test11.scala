package com.he


object Test11 {
  def main(args: Array[String]): Unit = {
    val c = 1
    val second = "14-11"
    val record = "{\"id\":\""+c+"\",\"createTime\":\""+second+"\",\"orderId\":\""+c+"\"}"
    val readable = s"""{"id":"$c","createTime":"$second","orderId":"2"}"""
    println(readable)

    for (i <- 0 to 1){
      println(i)
    }
  }

}
