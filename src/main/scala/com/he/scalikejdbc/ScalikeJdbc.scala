package com.he.scalikejdbc

import com.he.client.DbPool
import com.he.core.Logging
import com.he.utils.Config
import org.apache.spark.sql.SparkSession
import scalikejdbc.SQL

object ScalikeJdbc extends Logging{
  def main(args: Array[String]): Unit = {
    log.info("start")
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val data = (1 to 1000).toDF("val")
    data
      .repartition(8)
      .foreachPartition(part => {
          DbPool.init(Config.mysqlLocal)
          DbPool.usingDB(Config.mysqlLocalName)(db => {
            db.autoCommit(implicit session => {
              SQL("insert into he.user(name,age) values (?,?)")
                .batch(part.toList)
                .apply()
            })
          })
      })

    spark.close()
  }

}
