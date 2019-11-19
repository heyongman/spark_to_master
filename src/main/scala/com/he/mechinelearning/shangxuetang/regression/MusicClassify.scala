package com.he.mechinelearning.shangxuetang.regression

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{IndexToString, LabeledPoint, StringIndexer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Dataset, SparkSession}


/**
  * 音乐分类
  * 使用逻辑回归，将音乐进行傅里叶转换(取频域特征)，提取前1000个特征
  * 这里预测并不准确，考虑生成特征时随机提取，而不是取前1000个，或者对特征进行处理
  */
object MusicClassify {

  def main(args: Array[String]): Unit = {
    train()
//    predict()
  }

  /**
    * 训练模型并保存
    */
  def train(): Unit ={
    val spark = SparkSession.builder()
      .appName("LinearRegressionDemo")
      .master("local[4]")
      .getOrCreate()
    import spark.implicits._

//    读取多个特征数据
    val groups = "classical,jazz,country,pop,rock,metal,hiphop"
    val df = spark.read.textFile(s"/Users/he/work/bigdata/media/机器学习与数据挖掘/02-回归算法/资料/genres/{$groups}/*fft")
//    转换格式
    val value: Dataset[Music] = df.map(line => {
      val strings = line.split(" +")
      val label_str = strings(0).replaceFirst("\\d+", "")
      val features = strings.drop(1).map(_.toDouble)
      Music(label_str, Vectors.dense(features))
    })

    val Array(trainDS,testDS) = value.randomSplit(Array(0.8,0.2),1234)

    val stringIndexerModel = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("label")

    val logisticRegression = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("values")
      .setMaxIter(50)
//      .setRegParam(0.2) //正则化参数
//      .setElasticNetParam(0.3)
      .setFamily("multinomial") //多元、二元


    val indexToString = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("type")
        .setLabels(stringIndexerModel.fit(trainDS).labels)

//    建立管道
    val pipeline = new Pipeline()
      .setStages(Array(stringIndexerModel,logisticRegression,indexToString))

    val pipelineModel = pipeline.fit(trainDS)

    pipelineModel.transform(testDS).show(false)

//    交叉网格参数验证
//    val paramGrid = new ParamGridBuilder()
//      .addGrid(logisticRegression.maxIter, Array(40, 50, 60, 70))
//      .build()
//
//    val crossVal = new CrossValidator()
//      .setEstimator(pipeline)
//      .setEvaluator(new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse"))
//      .setEstimatorParamMaps(paramGrid)
//      .setNumFolds(3)

//    val cvModel = crossVal.fit(trainDS)
//
//    val lrBestModel = cvModel.bestModel.asInstanceOf[PipelineModel].stages(1).asInstanceOf[LogisticRegressionModel]
//    println(lrBestModel.extractParamMap())
//    println(lrBestModel.summary.accuracy)
//    println(lrBestModel.summary.weightedFMeasure)

    val summary = pipelineModel.stages(1).asInstanceOf[LogisticRegressionModel].summary
    println(summary.accuracy)
    pipelineModel.write.overwrite().save("/Users/he/proj/bigdata_proj/spark_to_master/src/main/resources/music_classify_model")
    println("模型保存完毕")

    spark.close()
  }


  /**
    * 加载模型进行预测
    */
  def predict(): Unit ={
    val spark = SparkSession.builder()
      .appName("LinearRegressionDemo")
      .master("local[4]")
      .getOrCreate()
    import spark.implicits._

//    读取预测音乐特征并转换
    val df1 = spark.read.textFile("/Users/he/work/bigdata/media/机器学习与数据挖掘/02-回归算法/资料/genres/heibao-wudizirong-remix.fft")
    val predictDF = df1
      .select(collect_list($"value".cast(DoubleType)))
      .map(line => {
        val arr = line.getAs[Seq[Double]](0).toArray
        LabeledPoint(0,Vectors.dense(arr)) //标记点，存储格式是LIBSVM和LIBLINEAR
      }).select("features")

    val pipelineModel = PipelineModel.load("/Users/he/proj/bigdata_proj/spark_to_master/src/main/resources/music_classify_model")
    pipelineModel
      .stages(1)
      .asInstanceOf[LogisticRegressionModel]
      .setFeaturesCol("features") //取出stage重新指定参数（下标从0开始）

    val res = pipelineModel.transform(predictDF)
    res.show()

    spark.close()
  }
}

case class Music(category:String,values:Vector)