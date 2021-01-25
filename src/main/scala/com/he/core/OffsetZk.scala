package com.he.core

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable

class OffsetZk(func:() => ZkClient) extends Logging with Serializable {

  lazy val zkClient:ZkClient = func()
  lazy val zkUtils:ZkUtils = ZkUtils.apply(zkClient,isZkSecurityEnabled = false)

  def getBeginOffset(topics:Seq[String], groupId:String): mutable.HashMap[TopicPartition, Long] = {
    val fromOffsets = mutable.HashMap.empty[TopicPartition,Long]
    val partitionMap = zkUtils.getPartitionsForTopics(topics)
    partitionMap.foreach{ topicPartitions =>
      val topic = topicPartitions._1
      val topicDirs = new ZKGroupTopicDirs(groupId,topic)
      topicPartitions._2.foreach{ partition =>
        val tp = new TopicPartition(topic,partition)
        val kafkaOffset = getOffsetForKafka(tp)
        val zkPath = s"${topicDirs.consumerOffsetDir}/$partition"
        zkUtils.makeSurePersistentPathExists(zkPath)
        Option(zkUtils.readData(zkPath)._1) match {
          case Some(zkOffset) =>
            if(zkOffset.toLong < kafkaOffset) fromOffsets += tp->kafkaOffset
            else fromOffsets += tp->zkOffset.toLong
          case None => fromOffsets += tp->kafkaOffset
        }
      }
    }
    fromOffsets
  }

  def saveEndOffset(offsetRanges:Array[OffsetRange],groupId:String): Unit = {
    offsetRanges.foreach{ offsetRange =>
      val topicDirs = new ZKGroupTopicDirs(groupId,offsetRange.topic)
      val zkPath = s"${topicDirs.consumerOffsetDir}/${offsetRange.partition}"
      zkUtils.updatePersistentPath(zkPath,offsetRange.untilOffset.toString)
    }
  }

  def getOffsetForKafka(topicPartition:TopicPartition,time: Long = OffsetRequest.EarliestTime): Long = {
    val brokerId = zkUtils.getLeaderForPartition(topicPartition.topic,topicPartition.partition).get
    val broker = zkUtils.getBrokerInfo(brokerId).get
    val endpoint = broker.getBrokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    val consumer = new SimpleConsumer(endpoint.host,endpoint.port,10000,100000,"getOffset")
    val tp = TopicAndPartition(topicPartition.topic,topicPartition.partition)
    val request= OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(time,1)))
    consumer.getOffsetsBefore(request).partitionErrorAndOffsets(tp).offsets.head
  }
}

object OffsetZk extends Logging {

  def apply(zkServer: String): OffsetZk = {
    val func = () => {
      val zkClient = ZkUtils.createZkClient(zkServer, 30000, 30000)
      sys.addShutdownHook{
        info("Execute hook thread: ZkManager")
        zkClient.close()
      }
      zkClient
    }
    new OffsetZk(func)
  }
}