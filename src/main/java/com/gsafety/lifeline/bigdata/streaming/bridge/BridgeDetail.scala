package com.gsafety.lifeline.bigdata.streaming.bridge

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util

import com.gsafety.lifeline.bigdata.hbase.HBaseConnection
import com.gsafety.lifeline.bigdata.util.AvroUtil
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{BufferedMutatorParams, Put}
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import java.util.{Collections, Date, Properties}

import com.gsafety.lifeline.bigdata.avro.SensorDetail
import com.gsafety.lifeline.bigdata.kafka.KafkaProducer
import com.gsafety.lifeline.bigdata.pojo.SensorDetailData
import kafka.producer.KeyedMessage
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
/**
  * Created by hadoop on 2017/9/14.
  */
object BridgeDetail {
  val logger = Logger.getLogger(BridgeDetail.getClass)
  var sample = 1
  val sampleTime = sample * 1000
  val ACCE = "ACCE"
  val LOWSUMARY="LOWSUMRY"
  val DISPMT="DISPMT"
  val DYNDEF="DYNDEF"
  val STRAIN="STRAIN"


  def createContext(prop:Properties) = {

    val topic = prop.getProperty("kafka.topic","zrrtest") //kafka topic
    val brokerList = prop.getProperty("kafka.broker.list","udap1:9092,udap2:9092,udap3:9092")
    val groupid= prop.getProperty("kafka.groupid","testzrr1")
    val interval = prop.getProperty("spark.interval","10").toInt
    val hbaseZookeeperPort = prop.getProperty("hbase.hbaseZookeeperPort","2181")
    val zookeeperquorum = prop.getProperty("hbase.zookeeperquorum","udap1,udap2,udap3")
    val checkpointPath = prop.getProperty("spark.checkpointPath","/BridgeDetail")

    //monitoring与topic对应情况
    val hashMap=new util.HashMap[String,String]()
    for(sensor<-prop.getProperty("acce","ACCE").split(",")){
      hashMap.put(sensor,ACCE)
    }
    for(sensor<-prop.getProperty("dyndef","DYNDEF").split(",")){
      hashMap.put(sensor,DYNDEF)
    }
    for(sensor<-prop.getProperty("dispmt","DISPMT").split(",")){
      hashMap.put(sensor,DISPMT)
    }
    for(sensor<-prop.getProperty("lowsumary","LOWSUMY").split(",")){
      hashMap.put(sensor,LOWSUMARY)
    }
    for(sensor<-prop.getProperty("strain","STRAIN").split(",")){
      hashMap.put(sensor,STRAIN)
    }

    val conf = new SparkConf().setAppName("BridgeDetail")
    //多个job并行运算条件.setMaster("local[4]")
    conf.set("spark.streaming.concurrentJobs", "4")
    conf.set("spark.scheduler.mode", "FAIR")
    //注册kyro序列化
    //确保aoolication kill 后接收的数据能被处理完在关闭
    //conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("INFO")
    val dstTopics=sc.broadcast(hashMap)
    val ssc = new StreamingContext(sc, Seconds(interval))
    sc.broadcast(hashMap)
   // ssc.checkpoint(checkpointPath)

    val kafkaParmas = Map[String, String](
      "metadata.broker.list" -> brokerList,
      "group.id" -> groupid)
    val topics = topic.split(",").toSet
    //读kafka数据并反序列化
    val message: InputDStream[(String, Array[Byte])] = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParmas, topics)

    val sensorDatas: DStream[SensorDetailData] = message.map(_._2).map(line => {

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
          recordPartitions.foreach(x=>print(_))
        //insertBatchToHbase("M","",recordPartitions,hbaseZookeeperPort,zookeeperquorum)
        logger.info("------------intoHbase-----------")
      })
    })
    ssc
  }

  /** *
    * 批量插入HBase
    */
  def insertBatchToHbase(cf: String, sampleType: String, recordPartitions: Iterator[SensorDetailData], hbaseZookeeperPort: String, zookeeperquorum: String): Unit = {
    try {
      var connection = HBaseConnection.getConnection

      if (connection == null) {

        HBaseConnection.createConnecion(hbaseZookeeperPort, zookeeperquorum)
        connection = HBaseConnection.getConnection
      }

      val tableName = TableName.valueOf("zrrTest")
      val params = new BufferedMutatorParams(tableName);
      params.writeBufferSize(1024 * 1024 * 5);

      val list = new util.ArrayList[Put]()

      val mutator = connection.getBufferedMutator(params)

      val simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
      val simpleFormats = new SimpleDateFormat("ss");
      var putCount = 0;
      for (data <- recordPartitions) {
        val ternal = data.terminal;
        val sensor = data.sensor
        val time = data.time
        val secondss = simpleFormats.format(time)
        val location = data.location
        val md5String = sensor + ternal
        val startrowkey = MD5Hash.getMD5AsHex(md5String.getBytes("utf-8"))
        val rowkey = startrowkey.substring(0, 6) + ":" + ternal + ":" + sensor + ":" + (Long.MaxValue - time)
        val put = new Put(Bytes.toBytes(rowkey))

        put.addColumn(cf.getBytes, "location".getBytes, Bytes.toBytes(location))
        put.addColumn(cf.getBytes, "seconds".getBytes, Bytes.toBytes(secondss))
        put.addColumn(cf.getBytes, "time".getBytes, Bytes.toBytes(simpleFormat.format(new Date(time))))
        put.addColumn(cf.getBytes, "rtime".getBytes, Bytes.toBytes(data.rtime))
        put.addColumn(cf.getBytes, "sensorType".getBytes, Bytes.toBytes(data.sensorType))
        put.addColumn(cf.getBytes, "dataType".getBytes, Bytes.toBytes(data.dataType))
        put.addColumn(cf.getBytes, "monitoring".getBytes, Bytes.toBytes(data.monitoring))
        put.addColumn(cf.getBytes, "level".getBytes, Bytes.toBytes(data.level))
        val valueString = ArraytoString(data.values)

        put.addColumn(cf.getBytes, "values".getBytes, Bytes.toBytes(valueString))


        list.add(put)
        if (list.size() > 200) {
          mutator.mutate(list)
          mutator.flush()
          logger.info("----------------------flush 5 puts----------------------")
          list.clear()
        }
        putCount=putCount+1
      }
      mutator.mutate(list)
      mutator.flush()
      logger.info("----------------------flush all puts count: "+putCount+" --------------------------")
      mutator.close()
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
    }

  }

  def ArraytoString(array: mutable.Buffer[Float]): String = {

    val valuesString = array.mkString(",")

    valuesString
  }
  def main(args: Array[String]): Unit = {
    val confFile = args(0)
    val prop = new Properties()
    prop.load(new FileInputStream(confFile))
    for(key<-prop.stringPropertyNames()) {
      logger.info("config:" + key + "->" + prop.get(key));
    }
    val checkpointPath = prop.getProperty("spark.checkpointPath","/BridgeDetail")
    val ssc = StreamingContext.getOrCreate(checkpointPath,
      () => {
        createContext(prop)
      })
    ssc.start()
    ssc.awaitTermination()
  }
}
