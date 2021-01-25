package com.he.spark2

import scala.collection.mutable

class LazyVarTest extends Serializable {
  val resource:Pool =  new Pool()
  def using[T](exec:Pool => T): T = exec(resource)
}

object LazyVarTest {
  def apply(): LazyVarTest = new LazyVarTest()

  def main(args: Array[String]): Unit = {
    val pool = LazyVarTest()
    val threads = new mutable.MutableList[Thread]
    for (i <- 1 to 5){
      val thread = new Thread {override def run() {
          pool.using(pool=>{
            println(i,pool)
          })
      }}
      threads += thread
    }

    for (thread <- threads){
      thread.start()
    }

  }
}
