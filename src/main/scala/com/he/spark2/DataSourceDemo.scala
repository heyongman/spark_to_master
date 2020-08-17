package com.he.spark2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, Filter, PrunedFilteredScan, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class DataSourceDemo extends CreatableRelationProvider with DataSourceRegister with PrunedFilteredScan{

  override def shortName(): String = ???

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = ???

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = ???
}
