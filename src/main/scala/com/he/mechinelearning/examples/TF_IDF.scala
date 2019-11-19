package com.he.mechinelearning.examples

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.{CountVectorizer, HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object TF_IDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("LogisticRegression")
      .master("local[4]")
      .getOrCreate()

    lda(spark)
//    tfIdf(spark)

  }

  def lda(spark: SparkSession): Unit ={
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val doc_class = spark
      .read
      .option("header", "true")
      .option("sep", "|")
      .csv("/Users/he/work/bigdata/media/SparkML机器学习实战/第02课/doc_class_1000.dat")
    val app_all = doc_class.withColumn("app_all",split($"myapp_word_all",", "))
    app_all.show(1,false)

    val countVectorizer = new CountVectorizer()
      .setInputCol("app_all")
      .setOutputCol("tf")
      .setMinDF(10) //[0,1)或[1,+∞)
      .setMinTF(2) //[0,1)或[1,+∞)
      .setVocabSize(1000)

    val idf = new IDF()
      .setInputCol("tf")
      .setOutputCol("tfidf")
      .setMinDocFreq(1)

    // We may alternatively specify parameters using a ParamMap,
    // which supports several methods for specifying parameters.
    val lr = new LogisticRegression()
    val paramMap = ParamMap(lr.maxIter -> 20)
      .put(lr.maxIter, 30)  // Specify 1 Param. This overwrites the original maxIter.
      .put(lr.regParam -> 0.1, lr.threshold -> 0.55)  // Specify multiple Params.


    val countVectorizerModel = countVectorizer.fit(app_all)
    val tf = countVectorizerModel.transform(app_all)
    //    countVectorizerModel.write.save("") // 模型和转换器都可以保存，为以后使用
    //    app_all.unpersist()

    val tfidf = idf.fit(tf).transform(tf)
    //    tf.unpersist()

    val lda = new LDA()
      .setOptimizer("em")
      .setK(10)
      .setMaxIter(10)
      .setFeaturesCol("tfidf")

    val model = lda.fit(tfidf)
    val transformed = model.transform(tfidf)

    val topics = model.describeTopics(10)

    val termMapTrans = udf({indices: mutable.WrappedArray[Int] => indices.map(countVectorizerModel.vocabulary)})
    //    Vector需要手动导spark的包
    val argmaxF = udf({arr: Vector => arr.argmax})
    val topicF = udf({(index:Int,arr:mutable.WrappedArray[String]) => arr(index)})
    val printStrArr = udf({s:mutable.WrappedArray[String] => s.filter(_ != "p").mkString(",")})

    topics
      .withColumn("terms",termMapTrans($"termIndices"))
      .select("topic","terms")
      .show(false)

    transformed
      .withColumn("dominatedTopics", argmaxF($"topicDistribution"))
      .withColumn("topic",topicF($"dominatedTopics",$"app_all"))
      .withColumn("text",printStrArr($"app_all"))
      .select("dominatedTopics","topic","text")
      .show(false)

    spark.close()
  }

  def tfIdf(spark: SparkSession): Unit ={
    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(32)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "features").show(false)

    spark.close()

  }

  def ldaDecisionTreeClassify(spark: SparkSession): Unit ={
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val doc_class = spark
      .read
      .option("header", "true")
      .option("sep", "|")
      .csv("/Users/he/work/bigdata/media/SparkML机器学习实战/第02课/doc_class_1000.dat")
    val app_all = doc_class.withColumn("app_all",split($"myapp_word_all",", "))
    app_all.show(1,false)

    val countVectorizer = new CountVectorizer()
      .setInputCol("app_all")
      .setOutputCol("tf")
      .setMinDF(50) //[0,1)或[1,+∞)
      .setMinTF(2) //[0,1)或[1,+∞)
      .setVocabSize(1000)

    val idf = new IDF()
      .setInputCol("tf")
      .setOutputCol("tfidf")
      .setMinDocFreq(10)

    val countVectorizerModel = countVectorizer.fit(app_all)
    val tf = countVectorizerModel.transform(app_all)
    val tfidf = idf.fit(tf).transform(tf)

    val lda = new LDA()
      .setOptimizer("em")
      .setK(10)
      .setMaxIter(10)
      .setFeaturesCol("tfidf")

    val model = lda.fit(tfidf)
    val transformed = model.transform(tfidf)

    val topics = model.describeTopics(10)

    val termMapTrans = udf({indices: mutable.WrappedArray[Int] => indices.map(countVectorizerModel.vocabulary)})
    //    Vector需要手动导spark的包
    val argmaxF = udf({arr: Vector => arr.argmax})
    val topicF = udf({(index:Int,arr:mutable.WrappedArray[String]) => arr(index)})
    val printStrArr = udf({s:mutable.WrappedArray[String] => s.filter(_ != "p").mkString(",")})

    topics
      .withColumn("terms",termMapTrans($"termIndices"))
      .select("topic","terms")
      .show(false)

    transformed
      .withColumn("dominatedTopics", argmaxF($"topicDistribution"))
      .withColumn("topic",topicF($"dominatedTopics",$"app_all"))
      .withColumn("text",printStrArr($"app_all"))
      .select("dominatedTopics","topic","text")
      .show(false)

    spark.close()
  }

}
