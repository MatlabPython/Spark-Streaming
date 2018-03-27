package com.gsafety.lifeline.bigdata.streaming.bridge
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import com.gsafety.lifeline.bigdata.avro.SensorDetail
import com.gsafety.lifeline.bigdata.hbase.HBaseConnection
import com.gsafety.lifeline.bigdata.kafka.KafkaProducer
import com.gsafety.lifeline.bigdata.pojo.SensorDetailData
import com.gsafety.lifeline.bigdata.util.AvroUtil
import com.gsafety.offset.StreamingZkOffsets
import kafka.message.MessageAndMetadata
import kafka.producer.KeyedMessage
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{BufferedMutatorParams, Put}
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.immutable.Iterable
import scala.collection.mutable
/**
  * Created by hadoop on 2017/9/14.
  */
object StreamingZkTest {
  val logger = Logger.getLogger(BridgeDetail.getClass)
  var sample = 1
  val sampleTime = sample * 1000
  def createContext(prop:Properties) = {
    /**
      *加载配置项，覆盖默认配置
      */
    val topic = prop.getProperty("kafka.topic","testzrr,testzrr1") //kafka topic
    val brokerList = prop.getProperty("kafka.broker.list","udap1:9092,udap5:9092")
    val groupid= prop.getProperty("kafka.groupid","zrrtest3")
    val interval = prop.getProperty("spark.interval","20").toInt
    val hbaseZookeeperPort = prop.getProperty("hbase.hbaseZookeeperPort","2181")
    val zookeeperquorum = prop.getProperty("hbase.zookeeperquorum","udap2,udap3,udap4")
    val checkpointPath = prop.getProperty("spark.checkpointPath","/tmp/streaming/checkpoint/bridgedetail1")
    val dsttopic=prop.getProperty("spark.topic","SparkHbase")
    /**
      * spark conf配置
      */
    val conf = new SparkConf().setAppName("BridgeTest")
    conf.registerKryoClasses(Array(classOf[SensorDetailData]))
    val sc = new SparkContext(conf)
    sc.setLogLevel("INFO")
    val ssc = new StreamingContext(sc, Seconds(interval))
    ssc.checkpoint(checkpointPath)
    /**
      * spark streaming kafka
      */
    val kafkaParams = Map[String, String](
      "bootstrap.servers"->brokerList,
      "zookeeper.connect" -> "udap2:2181/kafka",
      "zookeeper.connection.timeout.ms" -> "3000",
      "group.id" -> groupid)
    //      "auto.offset.reset"->"smallest")
    /**
      * 消费kafka数据并反序列化
      */
    val topics= topic.split(",").toSet
    var offsetRanges = Array[OffsetRange]()
    val streamingZkOffsets=new StreamingZkOffsets(topics,kafkaParams)
    val fromOffsets=streamingZkOffsets.getStartOffsets()
    val messageHandler = (mmd : MessageAndMetadata[String, Array[Byte]]) => (mmd.key(), mmd.message())
    val message: InputDStream[(String, Array[Byte])] = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder,(String,Array[Byte])](ssc, kafkaParams, fromOffsets, messageHandler)
    val sensorDatas: DStream[SensorDetailData] = message.transform(rdd=>{
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).map(_._2).map(line => {
      val sensor = AvroUtil.deserSensorDetail(line)
      val data = SensorDetailData(sensor.getLocation.toString, sensor.getTerminal.toString, sensor.getSensor.toString,
        sensor.getRtime, sensor.getSensorType.toString,
        sensor.getDataType.toString, sensor.getMonitoring.toString,
        sensor.getTime, sensor.getLevel,
        sensor.getValues.toBuffer[java.lang.Float].map(x=>x.toFloat))
      data
    })
    //明细数据写入Hbase
    sensorDatas.foreachRDD(rd => {
      rd.foreachPartition(recordPartitions => {
        insertBatchToHbaseAndKafka("M","",dsttopic,brokerList,recordPartitions,hbaseZookeeperPort,zookeeperquorum)
        logger.info("------------intoHbase-----------")
      })
      streamingZkOffsets.updateOffsets(offsetRanges)
    })
    ssc
  }
  def ArraytoString(array: mutable.Buffer[Float]): String = {
    val valuesString = array.mkString(",")
    valuesString
  }
  /** *
    * 批量插入HBase和Kafka
    */
  def insertBatchToHbaseAndKafka(cf: String, sampleType: String, dstTopic:String,brokerList: String,recordPartitions : Iterator[SensorDetailData], hbaseZookeeperPort: String, zookeeperquorum: String): Unit = {
    try {
      var connection = HBaseConnection.getConnection
      if (connection == null) {
        HBaseConnection.createConnecion(hbaseZookeeperPort, zookeeperquorum)
        connection = HBaseConnection.getConnection
      }
      val map = new util.concurrent.ConcurrentHashMap[String, util.ArrayList[Put]]()
      val simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
      val simpleFormats = new SimpleDateFormat("ss")
      var writeTokafka=false
      var tableNameStr = ""
      var producer2: KafkaProducer = null
      val mess = new util.ArrayList[KeyedMessage[String, Array[Byte]]]
      if(!(sampleType.equals("") || sampleType == null)){
        //需要双写Kafka与Hbase
        writeTokafka=true
        producer2=KafkaProducer.getInstance(brokerList)
      }
      var dadada = new SensorDetail()
      for (data <- recordPartitions) {
        val ternal = data.terminal
        val sensor = data.sensor
        val time = data.time
        val location = data.location
        val secondss = simpleFormats.format(time)
        val md5String = sensor + ternal
        val valueString = ArraytoString(data.values)
        if (!writeTokafka) {
          tableNameStr = "zrrTest"
        } else {
          /**
            * 双写kafka与hbase
            */
          tableNameStr = "zrrTest"
          dadada.setLocation(location)
          dadada.setTerminal(ternal)
          dadada.setSensor(sensor)
          dadada.setRtime(data.rtime)
          dadada.setSensorType(data.sensorType)
          dadada.setDataType(data.dataType)
          dadada.setMonitoring(data.monitoring)
          dadada.setTime(time)
          dadada.setLevel(data.level)
          val vava2: util.List[java.lang.Float] = data.values.map(xx => java.lang.Float.valueOf(xx))
          dadada.setValues(vava2)
          mess.add(new kafka.producer.KeyedMessage[String, Array[Byte]](dstTopic, AvroUtil.serializer(dadada)))
        }
        val tableName = TableName.valueOf(tableNameStr)
        val startrowkey = MD5Hash.getMD5AsHex(md5String.getBytes("utf-8"))
        var rowkey=""
        if(writeTokafka) {
          //采样数据
          rowkey = startrowkey.substring(0, 6) + ":" + ternal + ":" + sensor + ":" + (Long.MaxValue - time / sampleTime * sampleTime)
        }else{
          //明细数据
          rowkey = startrowkey.substring(0, 6) + ":" + ternal + ":" + sensor + ":" + (Long.MaxValue - time)
        }
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(cf.getBytes, "location".getBytes, Bytes.toBytes(location))
        put.addColumn(cf.getBytes, "seconds".getBytes, Bytes.toBytes(secondss))
        put.addColumn(cf.getBytes, "time".getBytes, Bytes.toBytes(simpleFormat.format(new Date(time))))
        put.addColumn(cf.getBytes, "rtime".getBytes, Bytes.toBytes(data.rtime))
        put.addColumn(cf.getBytes, "sensorType".getBytes, Bytes.toBytes(data.sensorType))
        put.addColumn(cf.getBytes, "dataType".getBytes, Bytes.toBytes(data.dataType))
        put.addColumn(cf.getBytes, "monitoring".getBytes, Bytes.toBytes(data.monitoring))
        put.addColumn(cf.getBytes, "level".getBytes, Bytes.toBytes(data.level))
        put.addColumn(cf.getBytes, "values".getBytes, Bytes.toBytes(valueString))
        val key = tableName.getNameAsString
        var list = map.get(key)
        if (list == null) {
          list = new util.ArrayList[Put](10000)
          list.add(put)
          map.put(key, list)
        } else {
          if (list.size() > 5000) {
            val params = new BufferedMutatorParams(tableName)
            params.writeBufferSize(2097152)
            val mutator = connection.getBufferedMutator(params)
            mutator.mutate(list)
            mutator.flush()
            mutator.close()
            logger.info("-------------------"+tableNameStr+"---flush 5000 puts----------------------")
            list.clear()
          }
          list.add(put)
        }
      }
      for ((tName, list) <- map) {
        val tableName = TableName.valueOf(tName)
        val params = new BufferedMutatorParams(tableName)
        params.writeBufferSize(2097152)
        val mutator = connection.getBufferedMutator(params)
        mutator.mutate(list)
        mutator.flush()
        logger.info("----------------"+tName+"------flush all puts--------------------------")
        mutator.close()
      }
      if(writeTokafka) {
        producer2.send(mess)
        logger.info("-----------------send data to kafka----------------------")
      }
    } catch {
      case ex: Exception => logger.error(ex)
    } finally {
      logger.info("close mutator success")
    }
  }
  def main(args: Array[String]): Unit = {
    val prop=new Properties
    //prop.setProperty("kafka.groupid",args(0))
    //    val ssc = StreamingContext.getOrCreate("/tmp/streaming/checkpoint/bridgedetail1", () => {
    //      createContext(prop)
    //    })
    val ssc=createContext(prop)
    ssc.start()
    ssc.awaitTermination()
  }
}