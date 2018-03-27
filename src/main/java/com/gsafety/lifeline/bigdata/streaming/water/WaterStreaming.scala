package com.gsafety.lifeline.bigdata.streaming.water

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util
import java.util.{Collections, Date, Properties}

import com.gsafety.lifeline.bigdata.hbase.HBaseConnection
import com.gsafety.lifeline.bigdata.pojo.SensorDetailData
import com.gsafety.lifeline.bigdata.util.AvroUtil
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by hadoop on 2017/9/4.
  */
object WaterStreaming {
  val logger = Logger.getLogger(WaterStreaming.getClass)
  var sample = 1
  val sampleTime = sample * 1000

  def createContext(prop: Properties) = {
    logger.info("---Creating new context---")
    val topic = prop.getProperty("kafka.topic","CLLSEN-WATER-HF-LOWSUMRY-PRO") //kafka topic
    val brokerList = prop.getProperty("kafka.broker.list","udap1:9092,udap5:9092")
    val groupid= prop.getProperty("kafka.groupid","water_1s")
    val interval = prop.getProperty("spark.interval","10").toInt
    val hbaseTables =prop.getProperty("hbase.tables","WATER_1S,WaterDetail").split(",")
    val hbaseZookeeperPort = prop.getProperty("hbase.hbaseZookeeperPort","2181")
    val zookeeperquorum = prop.getProperty("hbase.zookeeperquorum","udap2,udap3,udap4")
    val checkpointPath = prop.getProperty("spark.checkpointPath","/tmp/streaming/checkpoint/water1s")


    val conf = new SparkConf().setAppName("WaterStreaming")
    //注册kyro序列化
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    conf.registerKryoClasses(Array(classOf[SensorDetailData], classOf[util.ArrayList[Put]], classOf[Put], classOf[util.concurrent.ConcurrentHashMap[String, util.List[Put]]]))
    //多个job并行运算条件
    conf.set("spark.streaming.concurrentJobs", "4")
    conf.set("spark.scheduler.mode", "FAIR")
    conf.set("spark.rdd.compress", "true")
    //确保aoolication kill 后接收的数据能被处理完在关闭
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(interval))
    ssc.checkpoint(checkpointPath)

    val kafkaParmas = Map[String, String](
      "metadata.broker.list" -> brokerList,
      "group.id" -> groupid);
    val topics = topic.split(",").toSet
    val message = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParmas, topics)
    //数据反序列化并转换为case类
    val sensorDatas = message.map(_._2).map(line => {
      val sensor = AvroUtil.deserSensorDetail(line)
      val data = SensorDetailData(sensor.getLocation.toString, sensor.getTerminal.toString, sensor.getSensor.toString,
        sensor.getRtime, sensor.getSensorType.toString,
        sensor.getDataType.toString, sensor.getMonitoring.toString,
        sensor.getTime, sensor.getLevel,
        sensor.getValues.toBuffer[java.lang.Float].map(x=>x.toFloat))
      data
    })
    /**
      * 采样算法
      *
      */
    val orders = sensorDatas.filter(x => x.sensorType == "HPRESS").map(x => {
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
      for (clo <- 0 until dada(whichData).values.size) {
        val maxAndMin = new ListBuffer[ java.lang.Float ]
        var sum = 0.0
        for (row <- 0 until count) {
          maxAndMin.append(dada(row).values(clo))
          sum += dada(row).values(clo)
        }
        zong.append(Collections.max(maxAndMin).toFloat)
        zong.append(Collections.min(maxAndMin).toFloat)
        zong.append((sum / count).toFloat)
      }
      val dataResult = SensorDetailData(dada(whichData).location, dada(whichData).terminal, dada(whichData).sensor, dada(whichData).rtime, dada(whichData).sensorType, dada(whichData).dataType, dada(whichData).monitoring, dada(whichData).time, dada(whichData).level, zong)
      dataResult
    })
    //入到HBase，明细
    sensorDatas.foreachRDD(rd => {
      rd.foreachPartition(recordPartitions => {
        insertBatchHBase("M", hbaseTables(1), recordPartitions, hbaseZookeeperPort, zookeeperquorum)
        logger.info("------------intoHbase---"+hbaseTables(0)+"--------")
      })
    })
    //秒采样结果写入Hbase
    result.foreachRDD(rd => {
      rd.foreachPartition(recordPartitions => {
        insertBatchHBase("M", hbaseTables(0),recordPartitions, hbaseZookeeperPort, zookeeperquorum)
        logger.info("------------intoHbase-----------")
      })
    })
    //除了HPRESS外的其他类型直接写入Hbase
    sensorDatas.filter(_.sensorType!="HPRESS").repartition(1).foreachRDD(rd => {
      rd.foreachPartition(recordPartitions => {
        insertBatchHBase("M", hbaseTables(0),recordPartitions, hbaseZookeeperPort, zookeeperquorum)
        logger.info("------------intoHbase-----------")
      })
    })
    ssc
  }

  def insertBatchHBase(cf: String, hbaseTable: String, recordPartitions: Iterator[SensorDetailData], hbaseZookeeperPort: String, zookeeperquorum: String): Unit = {


    try {

      var connection = HBaseConnection.getConnection

      if (connection == null) {

        HBaseConnection.createConnecion(hbaseZookeeperPort, zookeeperquorum)
        connection = HBaseConnection.getConnection
      }
      val isSample=hbaseTable.equalsIgnoreCase("WATER_1S")
      val tableName = TableName.valueOf(hbaseTable)
      val params = new BufferedMutatorParams(tableName);

      params.writeBufferSize(1024 * 1024 * 5);

      val mutator = connection.getBufferedMutator(params);

      val list = new util.ArrayList[Put]()

      val simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
      val simpleFormats = new SimpleDateFormat("ss")
      var putCount = 0;
      for (data <- recordPartitions) {
        val ternal = data.terminal;
        val sensor = data.sensor
        val time = data.time
        val location = data.location
        val secondss = simpleFormats.format(time)
        val md5String = sensor + ternal
        val startrowkey = MD5Hash.getMD5AsHex(md5String.getBytes("utf-8"))
        var rowkey=""
        if(isSample){
          rowkey = startrowkey.substring(0, 6) + ":" + ternal + ":" + sensor + ":" + (Long.MaxValue - time/sampleTime*sampleTime)
        }else{
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
        val valueString = ArraytoString(data.values)
        put.addColumn(cf.getBytes, "values".getBytes, Bytes.toBytes(valueString))

        list.add(put)

        if (list.size() > 200) {
          mutator.mutate(list)
          mutator.flush()
          logger.info("-----------"+hbaseTable+"-----------flush 200 puts----------------------")
          list.clear()
        }
        putCount = putCount + 1;
      }
      mutator.mutate(list)
      mutator.flush()
      logger.info("-------------"+hbaseTable+"---------flush all puts count: "+putCount+" --------------------------")
      mutator.close()

    } catch {
      case ex: Exception => logger.error(ex)
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
    val checkpointPath = prop.getProperty("spark.checkpointPath","/tmp/streaming/checkpoint/water1s")
    val ssc = StreamingContext.getOrCreate(checkpointPath,
      () => {
        createContext(prop)
      })
    ssc.start()
    ssc.awaitTermination()
  }
}
