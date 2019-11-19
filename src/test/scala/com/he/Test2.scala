package com.he

import breeze.linalg.argmax
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.sql.SparkSession

object Test2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("LogisticRegression")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

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
//      .setMinDF(100)
//      .setMinTF(2)
//      .setVocabSize(1000)


    countVectorizer.fit(app_all).transform(app_all).show(false)

  }
}
