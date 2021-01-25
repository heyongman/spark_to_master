package com.he.uds

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException, Types}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object MysqlUpdate extends scala.AnyRef {
  implicit def sparkDataFrameFunctions(df: org.apache.spark.sql.DataFrame): SparkDataFrameFunctions = {
    new SparkDataFrameFunctions(df)
  }

  class SparkDataFrameFunctions(df: org.apache.spark.sql.DataFrame) extends scala.AnyRef with scala.Serializable {
    private type JDBCValueSetter = (PreparedStatement, Row, Int) => Unit

    private def makeSetter(dataType: DataType): JDBCValueSetter =
      dataType match {
      case IntegerType =>
        (stmt: PreparedStatement, row: Row, pos: Int) =>
          stmt.setInt(pos + 1, row.getInt(pos))

      case LongType =>
        (stmt: PreparedStatement, row: Row, pos: Int) =>
          stmt.setLong(pos + 1, row.getLong(pos))

      case DoubleType =>
        (stmt: PreparedStatement, row: Row, pos: Int) =>
          stmt.setDouble(pos + 1, row.getDouble(pos))

      case FloatType =>
        (stmt: PreparedStatement, row: Row, pos: Int) =>
          stmt.setFloat(pos + 1, row.getFloat(pos))

      case ShortType =>
        (stmt: PreparedStatement, row: Row, pos: Int) =>
          stmt.setInt(pos + 1, row.getShort(pos))

      case ByteType =>
        (stmt: PreparedStatement, row: Row, pos: Int) =>
          stmt.setInt(pos + 1, row.getByte(pos))

      case BooleanType =>
        (stmt: PreparedStatement, row: Row, pos: Int) =>
          stmt.setBoolean(pos + 1, row.getBoolean(pos))

      case StringType =>
        (stmt: PreparedStatement, row: Row, pos: Int) =>
          stmt.setString(pos + 1, row.getString(pos))

      case BinaryType =>
        (stmt: PreparedStatement, row: Row, pos: Int) =>
          stmt.setBytes(pos + 1, row.getAs[Array[Byte]](pos))

      case TimestampType =>
        (stmt: PreparedStatement, row: Row, pos: Int) =>
          stmt.setTimestamp(pos + 1, row.getAs[java.sql.Timestamp](pos))

      case DateType =>
        (stmt: PreparedStatement, row: Row, pos: Int) =>
          stmt.setDate(pos + 1, row.getAs[java.sql.Date](pos))

      case t: DecimalType =>
        (stmt: PreparedStatement, row: Row, pos: Int) =>
          stmt.setBigDecimal(pos + 1, row.getDecimal(pos))

      case _ =>
        (_: PreparedStatement, _: Row, pos: Int) =>
          throw new IllegalArgumentException(
            s"Can't translate non-null value for field $pos")
    }
    def createConnectionFactory(options: Map[String, String]): () => Connection = {
      val driverClass: String = options("driver")
      () => {
        Class.forName(driverClass)
        DriverManager.getConnection(options("url"), options("user"), options("password"))
      }
    }

    def savePartition(iterator: Iterator[Row],batchSize: Int,updateStmp:String, getConnection: () => Connection): Unit = {
      val conn = getConnection()
      var committed = false
      val rddSchema = df.schema
      val supportsTransactions = conn.getMetaData.supportsTransactions()
      try {
        conn.setAutoCommit(false)
        val stmt = conn.prepareStatement(updateStmp)
        val setters = rddSchema.fields.map(f => makeSetter(f.dataType))
//        val nullTypes = rddSchema.fields.map(f => getJdbcType(f.dataType, dialect).jdbcNullType)
        val numFields = rddSchema.fields.length

        try {
          var rowCount = 0

          stmt.setQueryTimeout(3)

          while (iterator.hasNext) {
            val row = iterator.next()
            var i = 0
            while (i < numFields) {
              if (row.isNullAt(i)) {
                stmt.setNull(i + 1, Types.NULL)
              } else {
                setters(i).apply(stmt, row, i)
              }
              i = i + 1
            }
            stmt.addBatch()
            rowCount += 1
            if (rowCount % batchSize == 0) {
              stmt.executeBatch()
              rowCount = 0
            }
          }
          if (rowCount > 0) {
            stmt.executeBatch()
          }
        } finally {
          stmt.close()
        }
        if (supportsTransactions) {
          conn.commit()
        }
        committed = true
        Iterator.empty
      } catch {
        case e: SQLException =>
          val cause = e.getNextException
          if (cause != null && e.getCause != cause) {
            // If there is no cause already, set 'next exception' as cause. If cause is null,
            // it *may* be because no cause was set yet
            if (e.getCause == null) {
              try {
                e.initCause(cause)
              } catch {
                // Or it may be null because the cause *was* explicitly initialized, to *null*,
                // in which case this fails. There is no other way to detect it.
                // addSuppressed in this case as well.
                case _: IllegalStateException => e.addSuppressed(cause)
              }
            } else {
              e.addSuppressed(cause)
            }
          }
          throw e
      } finally {
        if (!committed) {
          // The stage must fail.  We got here through an exception path, so
          // let the exception through unless rollback() or close() want to
          // tell the user about another problem.
          if (supportsTransactions) {
            conn.rollback()
          }
          conn.close()
        } else {
          // The stage must succeed.  We cannot propagate any exception close() might throw.
          try {
            conn.close()
          } catch {
            case e: Exception => println("Transaction succeeded, but closing failed", e)
          }
        }
      }
    }

    def getUpdateStmp(table: String): String ={
//      这个还要做字段名转义
      val columns = df.schema.map(x => s"`${x.name}").mkString(",")
      val placeholders = df.schema.map(x => "?").mkString(",")
      s"REPLACE INTO $table ($columns) VALUES ($placeholders)"
    }

    def updateToDB(table: String, cfg: Map[String, String]): scala.Unit = {
      val getConnection: () => Connection = createConnectionFactory(cfg)
      val batchSize = cfg.getOrElse("batchSize","1000").toInt
      val updateStmp = getUpdateStmp(table)

      df.foreachPartition(part => savePartition(part,batchSize,updateStmp, getConnection))

    }


  }


}