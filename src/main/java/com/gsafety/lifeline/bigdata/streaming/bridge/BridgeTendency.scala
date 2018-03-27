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
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Sean on 2017/9/21.
  * 计算桥梁十分钟趋势值，十分钟特征值
  */
object BridgeTendency {
  val logger = Logger.getLogger(BridgeTendency.getClass)
  var sample = 600
  val sampleTime = sample * 1000

  //创建spark环境
  def createContext(prop: Properties) = {

    val topic = prop.getProperty("kafka.topic","CLLSEN-BRIDGE-HF-ACCE-PRO,CLLSEN-BRIDGE-HF-DISPMT-PRO,CLLSEN-BRIDGE-HF-DYNDEF-PRO,CLLSEN-BRIDGE-HF-LOWSUMRY-PRO,CLLSEN-BRIDGE-HF-STRAIN-PRO") //kafka topic
    val brokerList = prop.getProperty("kafka.broker.list","udap1:9092,udap5:9092")
    val groupid= prop.getProperty("kafka.groupid","bridge_10m")
    val interval = prop.getProperty("spark.interval","600").toInt
    val hbaseZookeeperPort = prop.getProperty("hbase.hbaseZookeeperPort","2181")
    val zookeeperquorum = prop.getProperty("hbase.zookeeperquorum","udap2,udap3,udap4")
    val checkpointPath = prop.getProperty("spark.checkpointPath","/tmp/streaming/checkpoint/bridge10m")

    val conf = new SparkConf().setAppName("BridgeTendency")
    conf.set("spark.streaming.concurrentJobs", "4")
    conf.set("spark.scheduler.mode", "FAIR")
    //注册kyro序列化
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    conf.registerKryoClasses(Array(classOf[SensorDetailData], classOf[util.ArrayList[Put]], classOf[Put], classOf[util.concurrent.ConcurrentHashMap[String, util.List[Put]]]))
    conf.set("spark.rdd.compress", "true")
    //确保aoolication kill 后接收的数据能被处理完在关闭
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    conf.set("spark.speculation", "true")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(interval))
    ssc.checkpoint(checkpointPath)

    val kafkaParmas = Map[String, String](
      "metadata.broker.list" -> brokerList,
      "group.id" -> groupid);
    val topics = topic.split(",").toSet
    val message = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParmas, topics).map(_._2)

    val sensorDatas = message.map(line => AvroUtil.deserSensorDetail(line)).filter(sss => sss.getDataType.toString == "PV").map(nos10m => {
      val data = SensorDetailData(nos10m.getLocation.toString, nos10m.getTerminal.toString, nos10m.getSensor.toString,
        nos10m.getRtime, nos10m.getSensorType.toString,
        nos10m.getDataType.toString, nos10m.getMonitoring.toString,
        nos10m.getTime, nos10m.getLevel,
        nos10m.getValues.toBuffer[java.lang.Float].map(x=>x.toFloat))
      data
    })

    val sensorDatas2 = message.map(line => AvroUtil.deserSensorDetail(line)).filter(sss => sss.getDataType.toString == "S10M").map(s10m => {
      val data = SensorDetailData(s10m.getLocation.toString, s10m.getTerminal.toString, s10m.getSensor.toString,
        s10m.getRtime, s10m.getSensorType.toString,
        s10m.getDataType.toString, s10m.getMonitoring.toString,
        s10m.getTime, s10m.getLevel,
        s10m.getValues.toBuffer[java.lang.Float].map(x=>x.toFloat))
      data
    })

    /**
      * 采样算法
      */
    val orders = sensorDatas.map(x => {
      //Spark Streaming计算频率设定为10min
      val timeKey = x.time / sampleTime
      val key = x.location + "_" + x.terminal + "_" + x.sensor + "_" + timeKey
      (key, x)
    })
    val oss = orders.groupByKey()
    val result = oss.map(xx => {
      val count = xx._2.size
      val dada = xx._2.toList
      var zong = mutable.Buffer[Float]()
      //定义变量
      var whichData = 0

      for (va <- 0 until count - 1) {
        if (dada(va).level <= dada(va + 1).level) {
          whichData = va + 1
        } else {
          whichData = va
        }
      }
      val maxAndMin = new ListBuffer[java.lang.Float]
      var sum = 0.0
      for (row <- 0 until dada.size) {
        maxAndMin.append(dada(row).values(0))
        sum += dada(row).values(0)
      }
      zong.append(Collections.max(maxAndMin).toFloat)
      zong.append(Collections.min(maxAndMin).toFloat)
      zong.append((sum / count).toFloat)
      val dataResult = SensorDetailData(dada(whichData).location, dada(whichData).terminal, dada(whichData).sensor,
        dada(whichData).rtime, dada(whichData).sensorType,
        dada(whichData).dataType, dada(whichData).monitoring,
        dada(whichData).time, dada(whichData).level,
        zong)
      dataResult
    })
    sensorDatas2.foreachRDD(rd => {
      rd.foreachPartition(recordPartitions => {
        insertBatchToHbase("M", "FEATURE", brokerList,recordPartitions, hbaseZookeeperPort, zookeeperquorum)
        logger.info("------------intoHbase-BRIDGE_FEATURE_10M----------")
      })
    })
    result.foreachRDD(rd => {
      rd.foreachPartition(recordPartitions => {
        insertBatchToHbase("M", "TENDENCY",brokerList, recordPartitions, hbaseZookeeperPort, zookeeperquorum)
        logger.info("------------intoHbase-BRIDGE_TENDENCY_10M----------")
      })
    })
    ssc
  }

  def ArraytoString(array: mutable.Buffer[Float]): String = {
    val valuesString = array.mkString(",")
    valuesString
  }

  /** *
    * 批量插入HBase
    */
  def insertBatchToHbase(cf: String, sampleType: String,brokerList:String, recordPartitions: Iterator[SensorDetailData], hbaseZookeeperPort: String, zookeeperquorum: String): Unit = {
    try {


      var connection = HBaseConnection.getConnection

      if (connection == null) {

        HBaseConnection.createConnecion(hbaseZookeeperPort, zookeeperquorum)
        connection = HBaseConnection.getConnection
      }


      val map = new util.concurrent.ConcurrentHashMap[String, util.ArrayList[Put]]()

      val simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
      val simpleFormats = new SimpleDateFormat("ss");

      //----------插入kafka------------------------------------------------------------
      val producer2 = KafkaProducer.getInstance(brokerList);
      val mess = new util.ArrayList[KeyedMessage[String, Array[Byte]]]
      //----------插入kafka------------------------------------------------------------
      for (data <- recordPartitions) {

        val ternal = data.terminal
        val sensor = data.sensor
        val time = data.time
        val location = data.location
        val secondss = simpleFormats.format(time)
        val md5String = sensor + ternal
        val valueString = ArraytoString(data.values)
        //-------------------kafka------------------------
        var dadada = new SensorDetail()
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
        mess.add(new KeyedMessage[String, Array[Byte]]("CLLSEN-BRIDGE-HF-" + sampleType, AvroUtil.serializer(dadada)))
        //-------------------kafka------------------------
        val tableName = TableName.valueOf("BRIDGE_" + sampleType + "_10M")

        val startrowkey = MD5Hash.getMD5AsHex(md5String.getBytes("utf-8"))

        val rowkey = startrowkey.substring(0, 6) + ":" + ternal + ":" + sensor + ":" + (Long.MaxValue - time/sampleTime*sampleTime)


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

        val key = tableName.getNameAsString;


        var list = map.get(key)

        if (list == null) {
          list = new util.ArrayList[Put]()
          list.add(put)
          map.put(key, list)
        } else {

          if (list.size() > 200) {

            val params = new BufferedMutatorParams(tableName);

            val mutator = connection.getBufferedMutator(params);
            mutator.mutate(list)
            mutator.flush()
            mutator.close()
            logger.info("-------------------"+"BRIDGE_" + sampleType + "_10M"+"---flush 200 puts----------------------")
            list.clear()
          }
          list.add(put)
        }
      }

      for ((tName, list) <- map) {
        val tableName = TableName.valueOf(tName)
        val params = new BufferedMutatorParams(tableName);
        val mutator = connection.getBufferedMutator(params);
        mutator.mutate(list)
        mutator.flush()
        logger.info("----------------"+tName+"------flush all puts--------------------------")
        mutator.close()
      }
      producer2.send(mess)
      logger.info("-----------------send data to kafka----------------------")
    } catch {

      case ex: Exception => logger.error(ex)
    } finally {
      logger.info("close mutator success")
    }
  }

  def main(args: Array[String]): Unit = {
    val confFile = args(0)
    val prop = new Properties()
    prop.load(new FileInputStream(confFile))
    for(key<-prop.stringPropertyNames()) {
      logger.info("config:" + key + "->" + prop.get(key));
    }
    val checkpointPath = prop.getProperty("spark.checkpointPath","/tmp/streaming/checkpoint/bridge10m")
    val ssc = StreamingContext.getOrCreate(checkpointPath,
      () => {
        createContext(prop)
      })
    ssc.start()
    ssc.awaitTermination()
  }
}

