package com.gsafety.lifeline.bigdata.streaming.bridge

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util
import java.util.{Collections, Date, Properties}

import com.gsafety.lifeline.bigdata.avro.SensorDetail
import com.gsafety.lifeline.bigdata.hbase.HBaseConnection
import com.gsafety.lifeline.bigdata.kafka.KafkaProducer
import com.gsafety.lifeline.bigdata.pojo.SensorDetailData
import com.gsafety.lifeline.bigdata.util.AvroUtil
import kafka.producer.KeyedMessage
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
  * Created by hadoop on 2017/8/25.
  */
object BridgeStreaming {
  val logger = Logger.getLogger(BridgeStreaming.getClass)
  var sample = 1
  val sampleTime = sample * 1000
  //双写需要的topic名
  val ACCE = "CLLSEN-BRIDGE-HF-ACCE-HIVE"
  val LOWSUMARY = "CLLSEN-BRIDGE-HF-LOWSUMRY-HIVE"
  val DISPMT = "CLLSEN-BRIDGE-HF-DISPMT-HIVE"
  val DYNDEF = "CLLSEN-BRIDGE-HF-DYNDEF-HIVE"
  val STRAIN = "CLLSEN-BRIDGE-HF-STRAIN-HIVE"


  def createContext(prop: Properties) = {
    /**
      * 加载配置项，覆盖默认配置
      */
    val topic = prop.getProperty("kafka.topic", "CLLSEN-BRIDGE-HF-ACCE-PRO,CLLSEN-BRIDGE-HF-DISPMT-PRO,CLLSEN-BRIDGE-HF-DYNDEF-PRO,CLLSEN-BRIDGE-HF-LOWSUMRY-PRO,CLLSEN-BRIDGE-HF-STRAIN-PRO")
    //kafka topic
    val brokerList = prop.getProperty("kafka.broker.list", "udap2:9092,udap3:9092")
    val groupid = prop.getProperty("kafka.groupid", "bridge_1s")
    val interval = prop.getProperty("spark.interval", "20").toInt
    val hbaseZookeeperPort = prop.getProperty("hbase.hbaseZookeeperPort", "2181")
    val zookeeperquorum = prop.getProperty("hbase.zookeeperquorum", "udap1,udap2,udap3")
    val checkpointPath = prop.getProperty("spark.checkpointPath", "D:\\checkPoint\\B")

    /**
      * HashMap [monitoring,topic]对应情况，目前默认
      * 若原始数据更改monitoring字段，则修改对应配置项如下方式
      * acce=***,***(新monitoring字段可多个按“,”拼接)
      * dyndef=
      * dismpt=
      * lowsumary=
      * strain=
      */
    val hashMap = new util.HashMap[ String,String ]()
    for (sensor <- prop.getProperty("acce", "ACCE").split(",")) {
      hashMap.put(sensor, ACCE)
    }
    for (sensor <- prop.getProperty("dyndef", "DYNDEF").split(",")) {
      hashMap.put(sensor, DYNDEF)
    }
    for (sensor <- prop.getProperty("dispmt", "DISPMT").split(",")) {
      hashMap.put(sensor, DISPMT)
    }
    for (sensor <- prop.getProperty("lowsumary", "LOWSUMRY").split(",")) {
      hashMap.put(sensor, LOWSUMARY)
    }
    for (sensor <- prop.getProperty("strain", "STRAIN").split(",")) {
      hashMap.put(sensor, STRAIN)
    }

    /**
      * spark conf配置
      */
    val conf = new SparkConf().setAppName("BridgeStreaming20180330")//.setMaster("local[2]")
    //多个job并行运算条件
    conf.set("spark.streaming.concurrentJobs", "4")
    conf.set("spark.scheduler.mode", "FAIR")
    //确保aoolication kill 后接收的数据能被处理完在关闭
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(interval))
    ssc.checkpoint(checkpointPath)
    val dstTopics: Broadcast[ util.HashMap[ String,String ] ] = sc.broadcast(hashMap)
    /**
      * spark streaming kafka
      */
    val kafkaParmas = Map[ String, String ]("metadata.broker.list" -> brokerList, "group.id" -> groupid)
    val topics = topic.split(",").toSet
    /**
      * 消费kafka数据并反序列化
      */
    val message: InputDStream[ (String, Array[ Byte ]) ] = KafkaUtils.createDirectStream[ String, Array[ Byte ], StringDecoder, DefaultDecoder ](ssc, kafkaParmas, topics)

    val sensorDatas: DStream[ SensorDetailData ] = message.map(_._2).map(line => {
      val sensor = AvroUtil.deserSensorDetail(line)
      val data = SensorDetailData(sensor.getLocation.toString, sensor.getTerminal.toString, sensor.getSensor.toString, sensor.getRtime, sensor.getSensorType.toString, sensor.getDataType.toString, sensor.getMonitoring.toString, sensor.getTime, sensor.getLevel, sensor.getValues.toBuffer[ java.lang.Float ].map(x => x.toFloat))
      data

    })

    /**
      * 采样算法
      *
      */
    val orders = sensorDatas.filter(x => x.dataType == "PV").map(x => {
      //对取样数据按照秒进行区分
      val timeKey = x.time / sampleTime
      val key = x.location + "_" + x.terminal + "_" + x.sensor + "_" + timeKey
      (key, x)
    })

    val oss: DStream[ (String, Iterable[ SensorDetailData ]) ] = orders.groupByKey()
    val result: DStream[ SensorDetailData ] = oss.map(xx => {
      val count: Int = xx._2.size
      val dada = xx._2.toList
      var zong = mutable.Buffer[ Float ]()
      //定义变量
      var whichData = 0
      for (va <- 0 until count - 1) {
        if (dada(va).level <= dada(va + 1).level) {
          whichData = va + 1
        } else {
          whichData = va
        }
      }


        val maxAndMin = new ListBuffer[ java.lang.Float ]
        var sum = 0.0
        for (row <- 0 until count) {
          maxAndMin.append(dada(row).values.head)
          sum += dada(row).values.head
        }
        zong.append(Collections.max(maxAndMin).toFloat)
        zong.append(Collections.min(maxAndMin).toFloat)
        zong.append((sum / count).toFloat)

      val dataResult = SensorDetailData(dada(whichData).location, dada(whichData).terminal, dada(whichData).sensor, dada(whichData).rtime, dada(whichData).sensorType, dada(whichData).dataType, dada(whichData).monitoring, dada(whichData).time, dada(whichData).level, zong)
      dataResult

    })


    //明细数据写入Hbase
    sensorDatas.foreachRDD(rd => {
      rd.foreachPartition(recordPartitions => {
        val topics: util.HashMap[String,String] = dstTopics.value
        insertBatchToHbaseAndKafka("M", "", topics, brokerList, recordPartitions, hbaseZookeeperPort, zookeeperquorum)
        logger.info("------------intoHbase-----------")
      })
    })

    //秒采样结果写入Hbase以及Kafka
    result.foreachRDD(rd => {
      rd.foreachPartition(recordPartitions => {
        val topics: util.HashMap[String,String] = dstTopics.value

        insertBatchToHbaseAndKafka("M", "1S", topics, brokerList, recordPartitions, hbaseZookeeperPort, zookeeperquorum)
        logger.info("------------intoHbase-----------")
      })
    })

//    //除了PV外的其他类型直接写入Hbase
//    sensorDatas.filter(_.dataType!="PV").repartition(1).foreachRDD(rd => {
//      rd.foreachPartition(recordPartitions => {
//        val topics: util.HashMap[ String, String ] = dstTopics.value
//        insertBatchToHbaseAndKafka("M", "1S", topics, brokerList, recordPartitions, hbaseZookeeperPort, zookeeperquorum)
//        logger.info("------------intoHbase-----------")
//      })
//    })
    ssc
  }

  def ArraytoString(array: mutable.Buffer[ Float ]): String = {
    val valuesString = array.mkString(",")
    valuesString
  }

  /** *
    * 批量插入HBase和Kafka */
  def insertBatchToHbaseAndKafka(cf: String, sampleType: String, dstTopics: util.HashMap[ String, String ], brokerList: String, recordPartitions: Iterator[ SensorDetailData ], hbaseZookeeperPort: String, zookeeperquorum: String): Unit = {
    try {
      var connection = HBaseConnection.getConnection

      if (connection == null) {
        HBaseConnection.createConnecion(hbaseZookeeperPort, zookeeperquorum)
        connection = HBaseConnection.getConnection
      }


      val map = new util.concurrent.ConcurrentHashMap[ String, util.ArrayList[ Put ] ]()

      val simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
      val simpleFormats = new SimpleDateFormat("ss")
      var writeTokafka = false
      var tableNameStr = ""
      var producer2: KafkaProducer = null
      val mess = new util.ArrayList[ KeyedMessage[ String, Array[ Byte ] ] ]
      if (!(sampleType.equals("") || sampleType == null)) {
        //需要双写Kafka与Hbase
        writeTokafka = true
        producer2 = KafkaProducer.getInstance(brokerList)
      }
      var dadada = new SensorDetail()
      for (data <- recordPartitions) {
        val ternal = data.terminal
        println(ternal)
        val sensor = data.sensor
        val time = data.time
        val location = data.location
        val secondss = simpleFormats.format(time)
        val md5String = sensor + ternal
        val valueString = ArraytoString(data.values)
        if (!writeTokafka  && data.dataType != "S10M") {
          if(data.dataType == "PV"){
            tableNameStr = "BRIDGE" + "_" + ternal
          }else if(data.dataType == "FFT"){
            tableNameStr = "BRIDGE_FFT"
          }else{
            tableNameStr = "BRIDGE_OTHER_DATATYPE"
          }

        } else {
          /**
            * 双写kafka与hbase
            */
          tableNameStr = "BRIDGE" + "_" + ternal + "_" + sampleType
          dadada.setLocation(location)
          dadada.setTerminal(ternal)
          dadada.setSensor(sensor)
          dadada.setRtime(data.rtime)
          dadada.setSensorType(data.sensorType)
          dadada.setDataType(data.dataType)
          dadada.setMonitoring(data.monitoring)
          dadada.setTime(time)
          dadada.setLevel(data.level)
          val vava2: util.List[ java.lang.Float ] = data.values.map(xx => java.lang.Float.valueOf(xx))
          dadada.setValues(vava2)
          val topic = dstTopics.get(data.monitoring)//acce
          mess.add(new kafka.producer.KeyedMessage[ String, Array[ Byte ] ](topic, AvroUtil.serializer(dadada)))

        }
        val tableName = TableName.valueOf(tableNameStr)
        val startrowkey = MD5Hash.getMD5AsHex(md5String.getBytes("utf-8"))
        var rowkey = ""
        if (writeTokafka) {
          //采样数据
          rowkey = startrowkey.substring(0, 6) + ":" + ternal + ":" + sensor + ":" + (Long.MaxValue - time / sampleTime * sampleTime)

        } else {
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
          list = new util.ArrayList[ Put ]()
          list.add(put)
          map.put(key, list)
        } else {
          if (list.size() > 200) {
            val params = new BufferedMutatorParams(tableName)
            val mutator = connection.getBufferedMutator(params)
            mutator.mutate(list)
            mutator.flush()
            mutator.close()
            logger.info("-------------------" + tableNameStr + "---flush 200 puts----------------------")
            list.clear()
          }
          list.add(put)
        }
      }

      for ((tName, list) <- map) {
        val tableName = TableName.valueOf(tName)
        val params = new BufferedMutatorParams(tableName)
        val mutator = connection.getBufferedMutator(params)
        mutator.mutate(list)
        mutator.flush()
        logger.info("----------------" + tName + "------flush all puts--------------------------")
        mutator.close()
      }
      if (writeTokafka) {
        producer2.send(mess)
        logger.info("-----------------send data to kafka----------------------")
      }

    } catch {
      case ex: Exception => logger.error(ex)
    } finally {
      logger.info("close mutator success")
    }
  }

  def main(args: Array[ String ]): Unit = {
    //加载配置文件
    val confFile = args(0)
    val prop = new Properties()
    prop.load(new FileInputStream(confFile))
    for (key <- prop.stringPropertyNames()) {
      logger.info("config:" + key + "->" + prop.get(key));
    }
    val checkpointPath = prop.getProperty("spark.checkpointPath", "D:\\checkPoint\\B")
    val ssc = StreamingContext.getOrCreate(checkpointPath, () => {
      createContext(prop)
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
