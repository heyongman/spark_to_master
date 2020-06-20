package com.he.spark2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

/**
  * 有一组数据是用户观看观看视频，浏览网页的记录，三列：user_id,start_time,end_time
  * 用户从start_time开始浏览网页，到end_time结束浏览当前网页，
  * 我们想对用户的观看进行session划分，规定如果用户的访问间隔超过一天，那么就认为这两条记录是两个session的数据，否则是一个session的数据。
  * lag/lead 函数
  */
object SessionSplit {
  import org.apache.spark.sql.functions._

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test6").master("local[1]").getOrCreate()
    import spark.implicits._

    val df = Seq(("A","2019-01-01","2019-01-02"),("A","2019-01-02","2019-01-03"),("A","2019-01-04","2019-01-05"),("A","2019-01-08","2019-01-09"),("A","2019-01-09","2019-01-10")
      ,("B","2019-01-01","2019-01-02"),("B","2019-01-02","2019-01-03"),("B","2019-01-04","2019-01-05"),("B","2019-01-08","2019-01-09"),("B","2019-01-09","2019-01-10"))
      .toDF("user_id","start_time","end_time")

    df.show()

    val win = Window.partitionBy("user_id").orderBy("start_time")
//    那么在当前行就可以获取到前一行的end_time并且命名为befor_end_time
//    首先用lag将我们需要的上一列的值添加到到前列，然后用spark的简单函数对日期数据进行处理。
    //    然后我们添加一个flag对满足与不满足的event标记，如果当前行的start_time与前一行的befor_end_time减的值大于1，flag是1，如过<=1 flag是1.
    //    然后用分组函数sum对组内求和，这样同一个session的值是一样的，而且不同session会递增，超妙利用0+1=1， 0+0=0的很简单的原理。

    val dfWithLag = df.withColumn("befor_end_time",lag($"end_time",1).over(win))
    val dfDaysDiff = dfWithLag.withColumn("day",datediff($"start_time",$"befor_end_time"))
    val dfTag = dfDaysDiff.withColumn("flag",when($"day"<=1,0).otherwise(1))
    val dfSession = dfTag.withColumn("session",sum("flag").over(win))

    dfSession.show()

  }

}
