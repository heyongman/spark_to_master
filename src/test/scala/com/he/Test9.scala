package com.he

object Test9 {


  def main(args: Array[String]): Unit = {
    val strings = new Array[String](5)
    strings(1) = "a"
    strings(2) = "b"

    for (s <- strings){
      println(s)
    }
  }

}

