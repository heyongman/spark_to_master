//package com.yjyj.doubleEleven
//
//import java.sql
//import java.sql.DriverManager
//import com.yjyj.backUp.{GoodUtils, ProUtils, VipUtils}
//import com.yjyj.utils.{HbaseUtils, JsonUtil, PropertyUtil, TimeTrans}
//import org.apache.hadoop.hbase.TableName
//import org.apache.hadoop.hbase.client.Put
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.sql.{Row, SparkSession}
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import scala.collection.JavaConverters._
//import scala.collection.mutable.ArrayBuffer
//import scala.util.Try
//
//
///**
//  * @author stefan.qin(帅)
//  * @date 2019/9/27 9:53
//  * @describe  代码要优雅.jpg
//  */
//
//object DoubleElevenStreaming {
//
//  private val properties = PropertyUtil.getPro("streaming.properties")
//  private var newVipList:ArrayBuffer[String] = null
//
//
//  def main(args: Array[String]): Unit = {
//    var context = StreamingContext.getOrCreate(properties.getProperty("checkPointPathDoubleEleven"), functionToCreateContext)
//    while (true) {
//      context.start()
//      val bool: Boolean = context.awaitTerminationOrTimeout(TimeTrans.resetTimeLess)
//      if (!bool) {
//        context.stop(false, true)
//        context = functionToCreateContext()
//      }
//    }
//  }
//
//  def functionToCreateContext():StreamingContext= {
//
//    val spark:SparkSession = SparkSession
//      .builder()
//      .appName(this.getClass.getSimpleName).master("yarn")
//      .config("spark.streaming.backpressure.enabled", "true")
//      .config("spark.streaming.backpressure.initialRate", "200")
//      .getOrCreate()
//    val context = spark.sparkContext
//    val ssc = new StreamingContext(context, Seconds(properties.getProperty("timeIntervalDoubleEleven").toInt))
//    ssc.checkpoint(properties.getProperty("checkPointPathDoubleEleven"))
//
//    val value = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      PreferConsistent,
//      org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe[String, String](
//        properties.getProperty("kafkaTopicOperation").split(","), Map[String, Object](
//          "bootstrap.servers" -> properties.getProperty("kafkaBrokers"),
//          "key.deserializer" -> classOf[StringDeserializer],
//          "value.deserializer" -> classOf[StringDeserializer],
//          "group.id" -> properties.getProperty("kafkaGroupDoubleEleven"),
//          "auto.offset.reset" -> "latest",
//          "enable.auto.commit" -> (false: java.lang.Boolean)
//        ))
//    )
//
//    //    存储offset
//    var offsetRanges = Array[OffsetRange]()
//    val  valueOffset:DStream[ConsumerRecord[String, String]] = value.transform(rdd => {
//      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      value.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
//      rdd
//    })
//
//    val goodList = GoodUtils.getGoodInfo("/data/jars/doubleEleven/dwd_goods_info_sap_s_d.txt")
//    val proList = ProUtils.getProInfo("/data/jars/doubleEleven/province_shopBn.txt")
//    val vipList = VipUtils.getVipInfo("/data/jars/doubleEleven/vipMemberId.txt")
//
////    val goodList = GoodUtils.getGoodInfo("D:\\dwd_goods_info_sap_s_d.txt")
////    val proList = ProUtils.getProInfo("D:\\province_shopBn.txt")
////    val vipList = VipUtils.getVipInfo("D:\\vipMemberId.txt")
//
//    val valueToCal: DStream[Row] = valueOffset.transform(
//        rdd=>{
//          val good = rdd.context.broadcast(goodList)
//          val pro = rdd.context.broadcast(proList)
//          val vip = rdd.context.broadcast(vipList)
//          val newVip = rdd.context.broadcast(newVipList)
//        rdd.map(a=> {
//          if (JsonUtil.isJsonObject(a.value())) {
//            val jSONObject = JsonUtil.stringToJSONObject(a.value())
//            if (jSONObject.containsKey("channelsOrder") && jSONObject.containsKey("shopBn")
//              && jSONObject.containsKey("createTime") && jSONObject.containsKey("totalAmount")
//              && jSONObject.containsKey("shippingOrder") && jSONObject.containsKey("statStatus")
//            )
//            {
//              val shopBn = jSONObject.getString("shopBn")
//              val shopBnArray =
//                Array("newkhronghe","newb2capp","newgbck","duobao","b2capp","ecstoreb2b2c",
//                  "newbbcronghe","ptf","newbbc","yjj","sjswb","sjs","khxingjiushenqjd","zsy",
//                  "gjsc","brandMall","dzjg","jpmjg","wuliang","wxkb","yjj","zsgf")
//              if(shopBnArray.contains(shopBn)){
//                var channel = "其他"
//                if(shopBn=="newkhronghe"){
//                  channel = "B2C_APP(快喝)"
//                }else if(shopBn == "newgbck"){
//                  channel = "新隔壁仓库"
//                }else if(shopBn == "wxkb"){
//                  channel = "微信卡包"
//                }else if(shopBn == "yjj" || shopBn == "sjs"){
//                  channel = "内部侍酒师"
//                }else if(shopBn == "newbbcronghe"){
//                  channel = "官网"
//                }
//                //              统计特殊渠道订单和销售额
//                var specialOrder = ""
//                var orderBn = ""
//                if(jSONObject.containsKey("orderBn")){
//                  orderBn = jSONObject.getString("orderBn")
//                }
//                if(orderBn!=""){
//                  if("MS$".r.findAllIn(orderBn).toList.nonEmpty){
//                    specialOrder = "秒杀"
//                  }else if("MPRE$".r.findAllIn(orderBn).toList.nonEmpty){
//                    specialOrder = "预售"
//                  }else if("MCOL$".r.findAllIn(orderBn).toList.nonEmpty){
//                    specialOrder = "拼团"
//                  }
//                }
//                val statStatus = jSONObject.getInteger("statStatus")
//                val createTime = jSONObject.getString("createTime")
//
//                var storeNo = ""
//                if(jSONObject.containsKey("storeNo")){
//                  storeNo = jSONObject.getString("storeNo")
//                }
//
//                var memberId = ""
//                if (jSONObject.containsKey("memberAccount")) {
//                  memberId = jSONObject.getString("memberAccount")
//                }
//                val totalAmount = jSONObject.getString("totalAmount")
//
//                var shopName = ""
//                var province = ""
//                if(pro.value.contains(storeNo)) {
//                  shopName = pro.value(storeNo).head.shopName
//                  province = pro.value(storeNo).head.provinceManager
//                }
//
//                  val orderItems = jSONObject.getString("orderItems")
//                  var goodsModule = ""
//                  if (!orderItems.isEmpty && orderItems != "") {
//                    val orderItemsArray = JsonUtil.stringToJsonArray(orderItems)
//                    for (i <- 0 until orderItemsArray.size()) {
//                      val nObject = orderItemsArray.getJSONObject(i)
//                      val bn = nObject.getString("bn")
//                      val actualAmount = nObject.getString("actualAmount")
//                      val quantity = nObject.getString("quantity")
//                      if(bn!=""&&actualAmount!=""&&quantity!="")
//                        if (good.value.contains(bn)) goodsModule = goodsModule + "^" + good.value(bn).head.goods_name + ";" +
//                          good.value(bn).head.goodType + ";" + actualAmount + ";" + quantity
//                    }
//                  }
//                  var isVip = ""
//                  if(newVip.value==null){
//                    if(vip.value.contains(memberId))isVip = "1"
//                  }else{
//                    if(vip.value.contains(memberId) || newVip.value.contains(memberId)) isVip = "1"
//                  }
//
//                  if (statStatus == 1 && TimeTrans.isToday(createTime)) {
//                    Row.apply(createTime,totalAmount,memberId,channel,goodsModule,shopName,"1",specialOrder,province,isVip)
//                  }else null
//              }else null
//            } else if(jSONObject.containsKey("midMemberId") && jSONObject.containsKey("registerTime"))
//            {
//              val registerTime = jSONObject.getString("registerTime")
//              val midMemberId = jSONObject.getString("midMemberId")
//              if(TimeTrans.isToday(registerTime+"000")){
//                Row.apply(midMemberId,registerTime,"","","","","2","","","")
//              } else null
//            } else if(jSONObject.containsKey("midMemberId") && jSONObject.containsKey("expireDate")){
//              val midMemberId = jSONObject.getString("midMemberId")
//              Row.apply(midMemberId,"","","","","","3","","","")
//            } else if(jSONObject.containsKey("dts_create_timestamp") && jSONObject.containsKey("dts_paid_amount")
//              && jSONObject.containsKey("dts_source_channel") && jSONObject.containsKey("dts_depot_code")
//              && jSONObject.containsKey("dts_order_code") && jSONObject.containsKey("dts_special_order_type")
//            )
//            {
////              品牌商家门店排行
//              var shopName = ""
//              val  vendorName  = jSONObject.getString("dts_vendor_name")
//              if(vendorName!="-")shopName = vendorName else shopName=""
//              val totalAmount = jSONObject.getString("dts_paid_amount")
//
//              val createTime_1 =  jSONObject.getString("dts_create_timestamp")
//              val createTime = createTime_1.substring(0,createTime_1.length-3)
////              val orderCode: String = jSONObject.getString("dts_order_code")
//
////               自发货 4   代发货 5
//             val specialOrderType = jSONObject.getString("dts_special_order_type")
//              if(specialOrderType == "-" || specialOrderType == "presale_order"){
//                var memberId = ""
//                if(jSONObject.containsKey("dts_member_id")){
//                  memberId  =  jSONObject.getString("dts_member_id")
//                }
//                var channel = "其他"
//                val channel_1 = jSONObject.getString("dts_source_channel")
//                if(channel_1 == "1919kuaihe"){
//                  channel = "B2C_APP(快喝)"
//                }else if(channel_1 == "1919新PC官网"){
//                  channel = "官网"
//                }
//                //              统计特殊渠道订单和销售额
//                var specialOrder = ""
//                var orderBn = ""
//                if(jSONObject.containsKey("dts_order_code")){
//                  orderBn = jSONObject.getString("dts_order_code")
//                }
//                if(orderBn!=""){
//                  if("S$".r.findAllIn(orderBn).toList.nonEmpty){
//                    specialOrder = "秒杀"
//                  }else if("PRE$".r.findAllIn(orderBn).toList.nonEmpty){
//                    specialOrder = "预售"
//                  }else if("COL$".r.findAllIn(orderBn).toList.nonEmpty){
//                    specialOrder = "拼团"
//                  }
//                }
//
//                var isVip = ""
//                if(newVip.value==null){
//                  if(vip.value.contains(memberId))isVip = "1"
//                }else{
//                  if(vip.value.contains(memberId) || newVip.value.contains(memberId)) isVip = "1"
//                }
//                if(TimeTrans.isToday(createTime)){
//                  Row.apply(createTime,totalAmount,memberId,channel,"",shopName,"4",specialOrder,"",isVip)
//                }else null
//              }else if(specialOrderType == "substitute_delivery_order" || specialOrderType == "substitute_delivery_presale_order"){
//                if(TimeTrans.isToday(createTime)){
//                  Row.apply(createTime,totalAmount,"","","",shopName,"5","","","")
//                }else null
//              }else null
//            } else null
//          } else null
//         }
//        )
//      }).filter(a => a != null)
//      .persist(StorageLevel.MEMORY_AND_DISK_SER)
//
//        valueToCal.filter(a=> a.get(6)=="1" || a.get(6) == "4").filter(a=>a.get(2)!= "").map(a=> {("memberId",ArrayBuffer(a.get(2).toString))})
//      .union(valueToCal.filter(a=>a.get(6)=="2" && a.get(0)!= "").map(a=>("registerId",ArrayBuffer(a.get(0).toString))))
//      .union(valueToCal.filter(a=>a.get(6)=="1" && a.get(5)!= ""&& a.get(8)!= "").map(a=>("province1,"+a.get(8).toString,ArrayBuffer(a.get(5).toString))))
//      .union(valueToCal.filter(a=>a.get(0)!="" && a.get(6)=="3").map(a=>("vipMemberId",ArrayBuffer(a.get(0).toString))))
//      .reduceByKeyAndWindow((a:ArrayBuffer[String],b:ArrayBuffer[String]) => a++b,
//        (a:ArrayBuffer[String],b:ArrayBuffer[String]) => a--b,Seconds(60*60*24), Seconds(20))
//            .foreachRDD(
//              rdd=>{
//                if(!rdd.isEmpty()){
//                  var mysqlConn: sql.Connection = null
//                  var ps: sql.PreparedStatement = null
//                  Class.forName("com.mysql.jdbc.Driver").newInstance
//                    rdd.foreachPartition(
//                      partition=>{
//                        if(partition.nonEmpty) {
//                          try {
//                            mysqlConn = DriverManager.getConnection(
//                              properties.getProperty("mysqlUrl_1"),
//                              properties.getProperty("mysqlUser_1"), properties.getProperty("mysqlPassWord_1"))
//                            partition.foreach(
//                              a => {
//                                if (a._1 == "registerId") {
//                                  ps = mysqlConn.prepareStatement("INSERT INTO doubleEleven_person(name,count) VALUES (?,?) " +
//                                    "ON DUPLICATE KEY UPDATE count = ?")
//                                  ps.setString(1, a._1)
//                                  ps.setInt(2, a._2.distinct.size)
//                                  ps.setInt(3, a._2.distinct.size)
//                                  ps.executeUpdate()
//                                } else if (a._1 == "memberId") {
//                                  val distinct: ArrayBuffer[String] = a._2.distinct
//                                  ps = mysqlConn.prepareStatement("INSERT INTO doubleEleven_person(name,count) VALUES (?,?) " +
//                                    "ON DUPLICATE KEY UPDATE count = ?")
//                                  ps.setString(1, a._1)
//                                  ps.setInt(2, distinct.size)
//                                  ps.setInt(3, distinct.size)
//                                  ps.executeUpdate()
//
//                                  val conn = HbaseUtils.createCon()
//                                  val tDouElevenUsers = TableName.valueOf("doubleElevenUsers" + TimeTrans.NowDayDate())
//                                  val tUsers = conn.getTable(tDouElevenUsers)
//                                  HbaseUtils.createHTable(conn, "doubleElevenUsers" + TimeTrans.NowDayDate())
//
//                                  val java = distinct.map(a => {
//                                    val put = new Put(Bytes.toBytes(a))
//                                    put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("time"), Bytes.toBytes(TimeTrans.getEntryTime().toString))
//                                    put
//                                  }).toList.asJava
//                                  Try(tUsers.put(java)).getOrElse(tUsers.close())
//                                  tUsers.close()
//                                } else if("province1,".r.findFirstIn(a._1.toString).isDefined) {
//                                  ps = mysqlConn.prepareStatement("INSERT INTO doubleEleven_province(province,num) VALUES (?,?) " +
//                                    "ON DUPLICATE KEY UPDATE num = ? ")
//                                  ps.setString(1, a._1.split(",")(1))
//                                  ps.setInt(2, a._2.distinct.size)
//                                  ps.setInt(3, a._2.distinct.size)
//                                  ps.executeUpdate()
//                                }else if(a._1=="vipMemberId"){
//                                  val distinct: ArrayBuffer[String] = a._2.distinct
//                                  ps = mysqlConn.prepareStatement("INSERT INTO doubleEleven_person(name,count) VALUES (?,?) " +
//                                    "ON DUPLICATE KEY UPDATE count = ?")
//                                  ps.setString(1, a._1)
//                                  ps.setInt(2, distinct.size)
//                                  ps.setInt(3, distinct.size)
//                                  ps.executeUpdate()
//                                  newVipList = distinct
//                                }
//                              }
//                            )
//                          } catch {
//                            case t: Throwable => t.printStackTrace()
//                          } finally {
//                            if (ps != null) ps.close()
//                            if (mysqlConn != null) mysqlConn.close()
//                          }
//                        }
//                      }
//                    )
//                }
//            })
//
//
//       valueToCal.map(a => {if(a.get(6)== "1" || a.get(6)=="4"){("order", (1,a.get(1).toString.toDouble))} else null}).filter(a => a != null)
//      .union(valueToCal.filter(a=>a.get(6)=="1" || a.get(6)=="4").filter(a=>a.get(9)!="").map(a=>("vip", (1,a.get(1).toString.toDouble))))
//      .union(valueToCal.filter(a=>a.get(6)=="1" || a.get(6)=="4").filter(a=>a.get(3)!="").map(a=>("channel,"+a.get(3).toString,(1,a.get(1).toString.toDouble))))
//      .union(valueToCal.filter(a=>a.get(6)=="1" || a.get(6)=="4").filter(a=>a.get(7)!="").map(a=>("special,"+a.get(7).toString,(1,a.get(1).toString.toDouble))))
//      .union(valueToCal.filter(a=>a.get(6)=="1"  && a.get(8)!="").map(a=>("province,"+a.get(8).toString,(1,a.get(1).toString.toDouble))))
//      .union(valueToCal.filter(a=>a.get(6)=="4" || a.get(6)=="5").filter(a=>a.get(5)!="").map(a=> {("shop,"+a.get(5).toString,(1,a.get(1).toString.toDouble))}))
//       .union(
//           valueToCal.flatMap(a => {
//             val fuck = a.get(4).toString.split("\\^")
//              for (i <- 1 until fuck.length)
//              yield
//              if(fuck(i).split(";").length==4 && a.get(6) == "1"){
//              ("goodClass," + fuck(i).split(";")(1),
//             (fuck(i).split(";")(3).toInt,fuck(i).split(";")(2).toDouble))
//              }else ("", (1,1.toDouble))
//           }).filter(a => a._1 != ""))
//      .union(
//         valueToCal.flatMap(a => {
//            val fuck = a.get(4).toString.split("\\^")
//           for (i <- 1 until fuck.length)
//              yield
//              if(fuck(i).split(";").length==4 && a.get(6) == "1"){
//                ("good," + fuck(i).split(";")(1)+","+fuck(i).split(";")(0),
//                  (fuck(i).split(";")(3).toInt,fuck(i).split(";")(2).toDouble))
//              }else ("", (1,1.toDouble))
//         }).filter(a => a._1 != ""))
//        .reduceByKeyAndWindow((a:(Int,Double),b:(Int,Double)) => (a._1.toInt+b._1.toInt,a._2+b._2),
//          (a:(Int,Double),b:(Int,Double)) => (a._1.toInt-b._1.toInt,a._2-b._2)
//          ,Seconds(60*60*24), Seconds(20))
//        .transform(rdd=>{
//          rdd.map(a=>{
//            if("shop,".r.findFirstIn(a._1.toString).isEmpty && "good,".r.findFirstIn(a._1.toString).isEmpty) a else null
//          }).filter(a=>a!=null)
//            .union(
//            rdd.sparkContext.parallelize(rdd.map(a => {
//              if ("shop,".r.findFirstIn(a._1.toString).isDefined) (a._2._1, "count" + a._1) else null
//            }).filter(a => a != null).sortByKey(ascending = false).map(a => {
//              (a._2, (1, a._1.toDouble))
//            }).take(num=30)
//              .union(
//                rdd.map(a => {
//                  if ("shop,".r.findFirstIn(a._1.toString).isDefined) (a._2._2, "amount" + a._1) else null
//                }).filter(a => a != null).sortByKey(ascending = false).map(a => {
//                  (a._2, (1, a._1.toDouble))
//                }).take(num=30)
//              )))
//            .union(
//              rdd.map(a => {
//              if ("good,".r.findFirstIn(a._1.toString).isDefined) (a._1.split(",")(1), (a._1.split(",")(2), a._2._1, a._2._2)) else null
//            }).union(
//              rdd.map(a => {
//                if ("good,".r.findFirstIn(a._1.toString).isDefined) ("all", (a._1.split(",")(2), a._2._1, a._2._2)) else null
//              })
//            ).filter(a => a != null).groupByKey().map(a => {
//              (a._1, a._2.toList.sortBy(m => m._3).reverse.take(10))
//            }).flatMap(a => {
//              for (i <- a._2.indices)
//                yield ("good,"+a._1 + "," + a._2(i)._1, (a._2(i)._2, a._2(i)._3))
//            }))
//        }).foreachRDD(
//               rdd=>{
//                 if(!rdd.isEmpty()){
//                     var mysqlConn: sql.Connection = null
//                     var ps: sql.PreparedStatement = null
//                    Class.forName("com.mysql.jdbc.Driver").newInstance
//                     rdd.foreachPartition(
//                        partition=> {
//                          if(partition.nonEmpty) {
//                            try {
//                              mysqlConn = DriverManager.getConnection(
//                                properties.getProperty("mysqlUrl_1"),
//                                properties.getProperty("mysqlUser_1"), properties.getProperty("mysqlPassWord_1"))
//                              partition.foreach(a => {
//                                if (a._1 == "order") {
//                                  ps = mysqlConn.prepareStatement("INSERT INTO doubleEleven_order(time,count,amount) VALUES (?,?,?) " +
//                                    "ON DUPLICATE KEY UPDATE count = ?,amount=?")
//                                  ps.setString(1, TimeTrans.getCurHourTime())
//                                  ps.setInt(2, a._2._1)
//                                  ps.setDouble(3, a._2._2 / 100)
//                                  ps.setInt(4, a._2._1)
//                                  ps.setDouble(5, a._2._2 / 100)
//                                  ps.executeUpdate()
//                                } else if ("channel,".r.findAllIn(a._1).toList.nonEmpty) {
//                                  ps = mysqlConn.prepareStatement("INSERT INTO doubleEleven_channel(channel,count,amount) VALUES (?,?,?) " +
//                                    "ON DUPLICATE KEY UPDATE  count=?,amount = ?")
//                                  ps.setString(1, a._1.split(",")(1))
//                                  ps.setInt(2, a._2._1)
//                                  ps.setDouble(3, a._2._2 / 100)
//                                  ps.setInt(4, a._2._1)
//                                  ps.setDouble(5, a._2._2 / 100)
//                                  ps.executeUpdate()
//                                } else if ("special,".r.findAllIn(a._1).toList.nonEmpty) {
//                                  ps = mysqlConn.prepareStatement("INSERT INTO doubleEleven_specialOrder(orderName,count,amount) VALUES (?,?,?) " +
//                                    "ON DUPLICATE KEY UPDATE count =?,amount = ?")
//                                  ps.setString(1, a._1.split(",")(1))
//                                  ps.setInt(2, a._2._1)
//                                  ps.setDouble(3, a._2._2 / 100)
//                                  ps.setInt(4, a._2._1)
//                                  ps.setDouble(5, a._2._2 / 100)
//                                  ps.executeUpdate()
//                                } else if ("province,".r.findAllIn(a._1).toList.nonEmpty) {
//                                  ps = mysqlConn.prepareStatement("INSERT INTO doubleEleven_province(province,count,amount) VALUES (?,?,?) " +
//                                    "ON DUPLICATE KEY UPDATE count = ?,amount =? ")
//                                  ps.setString(1, a._1.split(",")(1))
//                                  ps.setInt(2, a._2._1)
//                                  ps.setDouble(3, a._2._2 / 100)
//                                  ps.setInt(4, a._2._1)
//                                  ps.setDouble(5, a._2._2 / 100)
//                                  ps.executeUpdate()
//                                } else if ("goodClass,".r.findAllIn(a._1).toList.nonEmpty) {
//                                  ps = mysqlConn.prepareStatement("INSERT INTO doubleEleven_Class(class,count,amount) VALUES (?,?,?) " +
//                                    "ON DUPLICATE KEY UPDATE count = ?,amount=?")
//                                  ps.setString(1, a._1.split(",")(1))
//                                  ps.setInt(2, a._2._1)
//                                  ps.setDouble(3, a._2._2 / 100)
//                                  ps.setInt(4, a._2._1)
//                                  ps.setDouble(5, a._2._2 / 100)
//                                  ps.executeUpdate()
//                                } else if ("countshop,".r.findAllIn(a._1).toList.nonEmpty) {
//                                  ps = mysqlConn.prepareStatement("INSERT INTO doubleEleven_shopBn(shopName,type,num) VALUES (?,?,?) " +
//                                    "ON DUPLICATE KEY UPDATE num = ?")
//                                  ps.setString(1, a._1.split(",")(1))
//                                  ps.setString(2, "1")
//                                  ps.setDouble(3, a._2._2)
//                                  ps.setDouble(4, a._2._2)
//                                  ps.executeUpdate()
//                                } else if ("amountshop,".r.findAllIn(a._1).toList.nonEmpty) {
//                                  ps = mysqlConn.prepareStatement("INSERT INTO doubleEleven_shopBn(shopName,type,num) VALUES (?,?,?) " +
//                                    "ON DUPLICATE KEY UPDATE num = ?")
//                                  ps.setString(1, a._1.split(",")(1))
//                                  ps.setString(2, "2")
//                                  ps.setDouble(3, a._2._2 / 100)
//                                  ps.setDouble(4, a._2._2 / 100)
//                                  ps.executeUpdate()
//                                } else if ("good,".r.findAllIn(a._1).toList.nonEmpty) {
//                                  ps = mysqlConn.prepareStatement("INSERT INTO doubleEleven_goods(goods_name,class,count,amount) VALUES (?,?,?,?) " +
//                                    "ON DUPLICATE KEY UPDATE count = ? ,amount=?")
//                                  ps.setString(1, a._1.split(",")(2))
//                                  ps.setString(2, a._1.split(",")(1))
//                                  ps.setInt(3, a._2._1)
//                                  ps.setDouble(4, a._2._2 / 100)
//                                  ps.setInt(5, a._2._1)
//                                  ps.setDouble(6, a._2._2 / 100)
//                                  ps.executeUpdate()
//                                }else if(a._1 == "vip"){
//                                  ps = mysqlConn.prepareStatement("INSERT INTO doubleEleven_specialOrder(orderName,count,amount) VALUES (?,?,?) " +
//                                    "ON DUPLICATE KEY UPDATE count =?,amount = ?")
//                                  ps.setString(1,"vip")
//                                  ps.setInt(2, a._2._1)
//                                  ps.setDouble(3, a._2._2 / 100)
//                                  ps.setInt(4, a._2._1)
//                                  ps.setDouble(5, a._2._2 / 100)
//                                  ps.executeUpdate()
//                                }
//                              })
//                            } catch {
//                              case t: Throwable => t.printStackTrace()
//                            } finally {
//                              if (ps != null) ps.close()
//                              if (mysqlConn != null) mysqlConn.close()
//                            }
//                          }
//                        }
//                     )
//                 }
//               }
//             )
//    ssc
//  }
//}
