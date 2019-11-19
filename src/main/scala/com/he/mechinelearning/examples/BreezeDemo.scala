package com.he.mechinelearning.examples

import breeze.linalg.{DenseMatrix, DenseVector, diag}

object BreezeDemo {
  def main(args: Array[String]): Unit = {

    println(DenseMatrix.zeros[Double](2, 3)) //全零矩阵
    println(DenseVector.zeros[Double](2)) //全零向量
    println(DenseMatrix.ones[Double](2,3)) //全1矩阵
    println(DenseMatrix.fill(4,3){3}) //填充
    println(DenseMatrix.rand[Double](2, 3)) //随机矩阵
    println(DenseVector.rand[Double](2)) //随机向量
    println(DenseVector.range(1, 10, 2))
    println(DenseMatrix.eye[Double](3)) //单位矩阵
    println(diag(DenseVector(1, 2, 3))) //对角矩阵
    println(DenseVector.range(1,10,2).t) //转置
    println(DenseMatrix((1,2,3),(4,5,6)))
    println(DenseMatrix((1,2,3),(4,5,6)).t) //转置
    println(DenseMatrix.tabulate(4,4){case (i,j) => i+j}) //从函数创建

    println()
    val a = DenseVector.range(1,10)
    println(a)
    println(a(0))
    println(a(5 to 1 by -1)) //倒序 步长-1
    println()
    val b = DenseMatrix.rand[Double](3,3)
    println(b)
    println(b(::, 2)) //选择矩阵第二列


  }
}
