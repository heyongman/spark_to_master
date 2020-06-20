package com.he.spark2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregateDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test10").master("local[*]").getOrCreate()
    import spark.implicits._

    val orders=Seq(
      ("深圳","钻石会员","钻石会员1个月",25),
      ("深圳","钻石会员","钻石会员1个月",25),
      ("深圳","钻石会员","钻石会员3个月",70),
      ("深圳","钻石会员","钻石会员12个月",300),
      ("深圳","铂金会员","铂金会员3个月",60),
      ("深圳","铂金会员","铂金会员3个月",60),
      ("深圳","铂金会员","铂金会员6个月",120),
      ("深圳","黄金会员","黄金会员1个月",15),
      ("深圳","黄金会员","黄金会员1个月",15),
      ("深圳","黄金会员","黄金会员3个月",45),
      ("深圳","黄金会员","黄金会员12个月",180),
      ("北京","钻石会员","钻石会员1个月",25),
      ("北京","钻石会员","钻石会员1个月",25),
      ("北京","铂金会员","铂金会员3个月",60),
      ("北京","黄金会员","黄金会员3个月",45),
      ("上海","钻石会员","钻石会员1个月",25),
      ("上海","钻石会员","钻石会员1个月",25),
      ("上海","铂金会员","铂金会员3个月",60),
      ("上海","黄金会员","黄金会员3个月",45)
    )
    //把seq转换成DataFrame
    val memberDF =orders.toDF("area","memberType","product","amount")

    memberDF.printSchema()
    memberDF.show()

//    多维聚合
//    https://blog.csdn.net/u011622631/article/details/84786777
//    https://blog.csdn.net/haiross/article/details/42102557
    memberDF
//      rollup(A ,B)等同于group by A,B并group by A并group by B并group by null后去重
      .rollup($"area",$"memberType",$"product")

//      cube(A, B)等同于group by A并group by B并group by null后去重，就是无顺序的排列组合分组
//      .cube($"area",$"memberType",$"product")
      .agg(sum($"amount"),grouping($"area"),grouping_id($"area",$"memberType",$"product") as "gid")
      .sort($"area".asc_nulls_last,$"memberType".asc_nulls_last,$"product".asc_nulls_last)
      .show(100)
  }
}
