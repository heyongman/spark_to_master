package com.he.bean

import scala.beans.BeanProperty

class Test1 {
  @BeanProperty var name:String = _

  @BeanProperty var age:Int = _

}

object Test1 {
  private val test = new Test1()
  test.getName


}