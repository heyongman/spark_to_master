package com.he.core

import scala.util.control.Exception.ignoring

/**
 * 借贷模式
 */
trait Borrow {

  type closeable = { def close(): Unit }

  def using[R <: closeable, T](resource: R)(execute: R => T): T = {
    try {
      execute(resource)
    } finally {
      ignoring(classOf[Throwable]) apply resource.close()
    }
  }
}
