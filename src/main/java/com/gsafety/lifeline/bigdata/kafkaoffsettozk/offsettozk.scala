//package com.gsafety.lifeline.bigdata.kafkaoffsettozk
//
//import com.gsafety.lifeline.bigdata.pojo.SensorDetailData
//import com.gsafety.lifeline.bigdata.util.AvroUtil
//import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
//import kafka.consumer.SimpleConsumer
//import kafka.message.MessageAndMetadata
//import kafka.serializer.{DefaultDecoder, StringDecoder}
//import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
//import org.I0Itec.zkclient.ZkClient
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.{SparkConf, rdd}
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import kafka.common.TopicAndPartition
//
//import scala.collection.JavaConversions._
//object offsettozk {
//  def createContext(checkpointDirectory: String) = {
//
//    println("create spark")
//    val topics = "CLLSEN-BRIDGE-HF-ACCE-PRO,CLLSEN-BRIDGE-HF-DISPMT-PRO,CLLSEN-BRIDGE-HF-DYNDEF-PRO,CLLSEN-BRIDGE-HF-LOWSUMRY-PRO,CLLSEN-BRIDGE-HF-STRAIN-PRO"
//    val group = "test_wnc"
//    val zkQuorum ="udap2:2181/kafka"
//    val brokerList = "udap1:9092,udap5:9092"
//    //    val Array(topics, group, zkQuorum,brokerList) = args
//    val sparkConf = new SparkConf().setAppName("KafkaTest").setMaster("local[2]")
//    //sparkConf.set("spark.streaming.kafka.maxRatePerPartition","1")
//    val ssc = new StreamingContext(sparkConf, Seconds(5))
//    //ssc.checkpoint(checkpointDirectory)
//    val topicsSet = topics.split(",").toSet
//    val kafkaParams = Map[String, String](
//      "metadata.broker.list" -> brokerList,
//      "group.id" -> group,
//      "zookeeper.connect"->zkQuorum
//    )
//
//
//    val topicDirs = new ZKGroupTopicDirs(group,topics) //创建一个 ZKGroupTopicDirs 对象，对保存
//    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"//获取 zookeeper 中的路径，这里会变成 /consumers/${group}/offsets/topic_name
//    val hostAndPort = "udap2:2181"
//    val zkClient = new ZkClient(hostAndPort)//zookeeper 的host 和 ip，创建一个 client
//    val children = zkClient.countChildren(zkTopicPath)
//    var kafkaStream :InputDStream[(String, Array[Byte])]  = null
//    var fromOffsets: Map[TopicAndPartition, Long] = Map()
//    if (children > 0) {
//      //---get partition leader begin----
//      val topicList = List(topics)
//      val req: TopicMetadataRequest = new TopicMetadataRequest(topicList,0)  //得到该topic的一些信息，比如broker,partition分布情况
//      val getLeaderConsumer: SimpleConsumer = new SimpleConsumer("10.5.4.28",8092,10000,10000,"OffsetLookup") // low level api interface
//      val res = getLeaderConsumer.send(req)  //TopicMetadataRequest   topic broker partition 的一些信息
//      val topicMetaOption = res.topicsMetadata.headOption
//      val partitions = topicMetaOption match{
//        case Some(tm) =>
//          tm.partitionsMetadata.map(pm=>(pm.partitionId,pm.leader.get.host)).toMap[Int,String]// 将结果转化为 partition -> leader 的映射关系
//        case None =>
//          Map[Int,String]()
//      }
//      //--get partition leader  end----
//      for (i <- 0 until children) {
//        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
//        val tp = TopicAndPartition(topics, i)
//        //---additional begin-----
//        val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime,1)))  // -2,1
//        val consumerMin = new SimpleConsumer(partitions(i),8092,10000,10000,"getMinOffset")
//        val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets
//        var nextOffset = partitionOffset.toLong
//        if(curOffsets.length >0 && nextOffset < curOffsets.head){  //如果下一个offset小于当前的offset
//          nextOffset = curOffsets.head
//        }
//        //---additional end-----
//        fromOffsets += (tp -> nextOffset)
//      }
//      val messageHandler = (mmd : MessageAndMetadata[String, Array[Byte]]) => (mmd.key(), mmd.message())
//      kafkaStream = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder,(String,Array[Byte])](ssc, kafkaParams, fromOffsets, messageHandler)
//    }else{
//      println("create")
//      kafkaStream = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)
//    }
//    println(fromOffsets)
//    var offsetRanges = Array[OffsetRange]()
//    kafkaStream.transform{
//      rdd=>offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//        rdd
//    }.map(msg=>msg._2).map(line => {
//
//      val sensor = AvroUtil.deserSensorDetail(line)
//      val data = SensorDetailData(sensor.getLocation.toString, sensor.getTerminal.toString, sensor.getSensor.toString,
//        sensor.getRtime, sensor.getSensorType.toString,
//        sensor.getDataType.toString, sensor.getMonitoring.toString,
//        sensor.getTime, sensor.getLevel,
//        sensor.getValues.toBuffer[java.lang.Float].map(x=>x.toFloat))
//      data
//    }).foreachRDD{rdd=>
//      for(offset <- offsetRanges ){
//        val zkPath = s"${topicDirs.consumerOffsetDir}/${offset.partition}"
//        println(offset.fromOffset.toString)
//        ZkUtils.updatePersistentPath(zkClient,zkPath,offset.fromOffset.toString)
//      }
//      rdd.foreachPartition(
//        message=>{
//          while(message.hasNext){
//            println(message.next())
//          }
//        })
//    }
//    ssc
//  }
//
//  def main(args: Array[String]) {
//
//    val checkpointDirectory = "F:\\BridgeDetail"
//    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
//      () => {
//        createContext(checkpointDirectory)
//      })
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}