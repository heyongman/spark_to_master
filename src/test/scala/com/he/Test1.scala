package com.he

object Test1 {
  def main(args: Array[String]): Unit = {
    (1 to 10) filter (_%2==0) map (x=>x*x) foreach println
  }
}
