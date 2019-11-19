package com.he.mechinelearning.shangxuetang.regression

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionSummary, LinearRegressionTrainingSummary}
import org.apache.spark.sql.{Dataset, SparkSession}

object LinearRegressionDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("LinearRegressionDemo")
      .master("local[4]")
      .getOrCreate()
    import spark.implicits._

//    -0.4307829,-1.63735562648104 -2.00621178480549 -1.86242597251066 -1.02470580167082 -0.522940888712441 -0.863171185425945 -1.04215728919298 -0.864466507337306
    val lpsa = spark.read.text("/Users/he/work/bigdata/media/机器学习与数据挖掘/02-回归算法/代码/SparkMLlib/lpsa.data")
    val data: Dataset[LabeledPoint] = lpsa.map(x => {
      val splits = x.getString(0).split(",")
      assert(splits.size == 2)
      val label = splits(0).toDouble
      val features = splits(1).split(" ").map(_.toDouble)
      LabeledPoint(label.toDouble, Vectors.dense(features))
    })

    val Array(trainingData, testData) = data.randomSplit(Array(0.8,0.2),1234) //拆分数据集

    val lrModel = new LinearRegression()
        .setFeaturesCol("features")
        .setLabelCol("label")
        .setMaxIter(1000) //最大迭代次数
        .setFitIntercept(true) //是否有截距
        .setRegParam(0.1) //正则化参数
        .setElasticNetParam(0.8) //弹性网络混合参数
        .fit(trainingData)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}") //系数 截距
    // Summarize the model over the training set and print out some metrics
    val trainingSummary: LinearRegressionTrainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}") //总迭代次数
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]") //客观历史
    trainingSummary.residuals.show() //残差（实际观察值与估计值之间的差）
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}") //均方根误差(标准差)
//    println(s"MSE: ${trainingSummary.meanSquaredError}")
    println(s"r2: ${trainingSummary.r2}") //决定系数（是相关系数的平方）
    println()

    val predictSummary: LinearRegressionSummary = lrModel.evaluate(testData)
    println(s"RMSE：${predictSummary.rootMeanSquaredError}")
//    println(s"MSE：${predictSummary.meanSquaredError}")
    println(s"r2：${predictSummary.r2}")
    predictSummary.residuals.show()


    val frame = lrModel
      .setFeaturesCol("value")
      .transform(testData.select($"features".alias("value")))
    frame.show()

    spark.close()
  }

}
