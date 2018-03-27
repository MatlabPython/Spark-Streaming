package com.gsafety.lifeline.bigdata.kafka
import java.util.Properties

import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig
import kafka.utils.{Logging, ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkException
import org.apache.spark.streaming.kafka.OffsetRange
/**
  * Created with Intellij IDEA.
  * User: naichun
  * Date: 2018-01-09
  * Time: 14:03
  */
class StreamingZkOffsets(topics: Set[String], kafkaParams: Map[String, String]) extends Logging {
  // Kafka connection properties
  val props = new Properties()
  kafkaParams.foreach(param => props.put(param._1, param._2))
  val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
  var isFromBeginning = false
  if (reset == Some("smallest")) {
    isFromBeginning = true
  }
  val brokers = kafkaParams.get("metadata.broker.list")
    .orElse(kafkaParams.get("bootstrap.servers"))
    .getOrElse(throw new SparkException(
      "Must specify metadata.broker.list or bootstrap.servers"))
  val consumerConfig = new ConsumerConfig(props)
  val zkServers = consumerConfig.zkConnect
  val groupId = consumerConfig.groupId
  val kafkaCluster = new KafkaCluster(kafkaParams)
  val zkClient = new ZkClient(zkServers, consumerConfig.zkSessionTimeoutMs, consumerConfig.zkConnectionTimeoutMs, new StringSerializer())
  val zkUtils=ZkUtils(zkClient,false)
  /**
    * 获取启动时所需offset位置
    *
    * @return Map[TopicAndPartition, Long]
    */
  def getStartOffsets(): Map[TopicAndPartition, Long] = {
    //新的groupId
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    for (topic <- topics) {
      if (!zkClient.exists("/consumers/" + groupId + "/offsets/" + topic)) {
        fromOffsets ++= getFromOffsets(kafkaCluster, kafkaParams, topics)
        logger.info("create new groupId:" + groupId + ",topic:" + topic)
      } else {
        val topicAndPartition = KafkaCluster.checkErrors(kafkaCluster.getPartitions(Set(topic)))
        val topicPartitionAndOffset = readZkOffsets(groupId, topicAndPartition)
        fromOffsets ++= checkOffsets(kafkaCluster, topicPartitionAndOffset)
      }
    }
    logger.info("Streaming start with offsets:" + fromOffsets)
    fromOffsets
  }
  /**
    * 更新offset
    *
    * @param offsetRanges
    */
  def updateOffsets(offsetRanges: Array[OffsetRange]): Unit = {
    for (offsetRange <- offsetRanges) {
      val topic = offsetRange.topic
      val partition = offsetRange.partition
      val offset = offsetRange.untilOffset
      val topicDirs = new ZKGroupTopicDirs(groupId, topic)
      zkUtils.updatePersistentPath(topicDirs.consumerOffsetDir + "/" + partition, offset.toString)
      logger.info("Streaming update offsets" + offsetRange.toString())
    }
  }
  /**
    * 关闭连接
    */
  def close(): Unit = {
    zkClient.close()
  }
  /**
    * 新groupId根据配置自动获取offset情况
    *
    * @param kc
    * @param kafkaParams
    * @param topics
    * @return Map[TopicAndPartition, Long]
    */
  private def getFromOffsets(kc: KafkaCluster,
                             kafkaParams: Map[String, String],
                             topics: Set[String]
                            ): Map[TopicAndPartition, Long] = {
    val result = for {
      topicPartitions <- kc.getPartitions(topics).right
      leaderOffsets <- (if (isFromBeginning) {
        kc.getEarliestLeaderOffsets(topicPartitions)
      } else {
        kc.getLatestLeaderOffsets(topicPartitions)
      }).right
    } yield {
      leaderOffsets.map { case (tp, lo) =>
        (tp, lo.offset)
      }
    }
    KafkaCluster.checkErrors(result)
  }
  /**
    * 从zk中获取offset
    *
    * @param groupId
    * @param topicAndPartitions
    * @return Map[TopicAndPartition, Long]
    */
  private def readZkOffsets(groupId: String, topicAndPartitions: Set[TopicAndPartition]): Map[TopicAndPartition, Long] = {
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    val topic = topicAndPartitions.head.topic
    val topicDirs = new ZKGroupTopicDirs(groupId, topic)
    for (topicAndPartition <- topicAndPartitions) {
      val offset = zkUtils.readDataMaybeNull(s"${topicDirs.consumerOffsetDir}/${topicAndPartition.partition}")._1 match {
        case Some(s) => s.toLong
        case None => -1L
      }
      fromOffsets += (topicAndPartition -> offset)
    }
    logger.info("Read " + groupId + "-" + topic + " offsets:" + fromOffsets)
    fromOffsets
  }
  /**
    * 判断获取的offset是否有效,并修正
    *
    * @param kc
    * @param topicPartitionAndOffset
    * @return Map[TopicAndPartition, Long]
    */
  private def checkOffsets(
                            kc: KafkaCluster,
                            topicPartitionAndOffset: Map[TopicAndPartition, Long]): Map[TopicAndPartition, Long] = {
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    val topics: Set[TopicAndPartition] = topicPartitionAndOffset.keySet
    val smallestOffsets = getSmallestOffsets(kc,topics)
    val largestOffsets = getLargestOffsets(kc, topics)
    if (isFromBeginning) {
      fromOffsets ++= smallestOffsets
    } else {
      fromOffsets ++= largestOffsets
    }
    for {
      low <- smallestOffsets
      high <- largestOffsets
    } {
      if (low._1.toString() == high._1.toString() && low._2 <= topicPartitionAndOffset(low._1) && high._2 >= topicPartitionAndOffset(high._1)) {
        fromOffsets += (low._1 -> topicPartitionAndOffset(low._1))
        logger.info("add an invalid offset:" + low._1 + "-" + topicPartitionAndOffset(low._1))
      }
    }
    fromOffsets
  }
  private def getSmallestOffsets(kc: KafkaCluster, topicAndPartitions: Set[TopicAndPartition]): Map[TopicAndPartition, Long] = {
    KafkaCluster.checkErrors(kc.getEarliestLeaderOffsets(topicAndPartitions)).map(x => (x._1, x._2.offset))
  }
  private def getLargestOffsets(kc: KafkaCluster, topicAndPartitions: Set[TopicAndPartition]): Map[TopicAndPartition, Long] = {
    KafkaCluster.checkErrors(kc.getLatestLeaderOffsets(topicAndPartitions)).map(x => (x._1, x._2.offset))
  }
}