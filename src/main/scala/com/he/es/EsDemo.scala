package com.he.es

import java.util.concurrent.CountDownLatch

import com.he.client.DbPool
import com.he.uds.MysqlUpdate
import com.he.utils.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.sql._
import com.he.uds.MysqlUpdate._

import scala.collection.mutable

object EsDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaIntegration")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")
//      .config("spark.serialize","org.apache.spark.serializer.KryoSerializer")
//      .config("spark.kryo.registrationRequired","true")
      .getOrCreate()
    import spark.implicits._

    val source = spark.read
      .option("es.nodes", "10.197.236.193:9200,10.197.236.194:9200,10.197.236.200:9200,10.197.236.201:9200")
      .option("es.mapping.date.rich",value = false)
      .format("es")
      .load("full_text_index_c07960578305439cb78fccf19ffe78a3")

    source.printSchema()
    source
      .groupBy("fieldName")
      .agg(count("db") as "count")
//      通过es.mapping.id指定id，通过es.write.operation=upsert来根据id更新
      .saveToEs("he_test/_doc",Map("es.nodes" -> "10.197.236.193:9200,10.197.236.194:9200,10.197.236.200:9200,10.197.236.201:9200"))


//      .show()

    new CountDownLatch(1).await()
    spark.close()
  }

}
