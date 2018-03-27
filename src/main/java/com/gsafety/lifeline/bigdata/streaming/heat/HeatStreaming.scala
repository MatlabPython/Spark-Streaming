package com.gsafety.lifeline.bigdata.streaming.heat
import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import com.gsafety.lifeline.bigdata.hbase.HBaseConnection
import com.gsafety.lifeline.bigdata.pojo.SensorDetailData
import com.gsafety.lifeline.bigdata.util.AvroUtil
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{BufferedMutatorParams, Put}
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import scala.collection.JavaConversions._

import scala.collection.mutable
/**
  * Created with Intellij IDEA.
  * User: naichun
  * Date: 2017-12-06
  * Time: 16:23
  */
object HeatStreaming {

  val logger = Logger.getLogger(HeatStreaming.getClass);
  //采样时间
  var sample = 1
  //采样时间为秒
  val sampleTime = sample * 1000


  def createContext(prop: Properties) = {
    logger.info("---Creating new context---")
    val topic = prop.getProperty("kafka.topic","CLLSEN-HEAT-HF-LOWSUMRY-PRO") //kafka topic
    val brokerList = prop.getProperty("kafka.broker.list","udap1:9092,udap5:9092")
    val groupid= prop.getProperty("kafka.groupid","heat_1s")
    val interval = prop.getProperty("spark.interval","10").toInt
    val hbaseTables =prop.getProperty("hbase.tables","HEAT_1S,HEAT").split(",")
    val hbaseZookeeperPort = prop.getProperty("hbase.hbaseZookeeperPort","2181")
    val zookeeperquorum = prop.getProperty("hbase.zookeeperquorum","udap2,udap3,udap4")
    val checkpointPath = prop.getProperty("spark.checkpointPath","/tmp/streaming/checkpoint/heat1s")


    val conf = new SparkConf().setAppName("HeatStreaming") //.setMaster("local[2]")
    //注册kyro序列化
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    conf.registerKryoClasses(Array(classOf[SensorDetailData], classOf[util.ArrayList[Put]], classOf[Put], classOf[util.concurrent.ConcurrentHashMap[String, util.List[Put]]]))
    conf.set("spark.rdd.compress", "true")
    //多个job并行运算条件
    conf.set("spark.streaming.concurrentJobs", "4")
    conf.set("spark.scheduler.mode", "FAIR")
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

    val sensorDatas = message.map(line => {
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
      */

    val orders = sensorDatas.map(x => {
      //对取样数据求除数
      val timeKey = x.time / sampleTime;
      val key = x.location + "_" + x.terminal + "_" + x.sensor + "_" + timeKey
      (key, x)
    })
    //根据相同的Key聚合求最小的时间
    val os = orders.reduceByKey((data1: SensorDetailData, data2: SensorDetailData) => {
      if (data2.time <= data1.time) {
        data2
      } else {
        data1
      }
    })

    //入到HBase
    os.map(x => x._2).foreachRDD(rd => {
      rd.foreachPartition(recordPartitions => {
        insertBatchToHbase("M", hbaseTables(0), recordPartitions, hbaseZookeeperPort, zookeeperquorum)
        logger.info("------------intoHbase--"+hbaseTables(0)+"---------")
      })
    })
    //插入hbase,明细
    sensorDatas.foreachRDD(rd => {
      rd.foreachPartition(recordPartitions => {
        insertBatchToHbase("M", hbaseTables(1), recordPartitions, hbaseZookeeperPort, zookeeperquorum)
        logger.info("------------intoHbase---"+hbaseTables(1)+"--------")
      })
    })
    ssc
  }

  /** *
    * 批量插入HBase
    */
  def insertBatchToHbase(cf: String, hbaseTable: String, recordPartitions: Iterator[SensorDetailData], hbaseZookeeperPort: String, zookeeperquorum: String): Unit = {
    try {
      var connection = HBaseConnection.getConnection

      if (connection == null) {

        HBaseConnection.createConnecion(hbaseZookeeperPort, zookeeperquorum)
        connection = HBaseConnection.getConnection
      }

      val tableName = TableName.valueOf(hbaseTable)
      val params = new BufferedMutatorParams(tableName)
      params.writeBufferSize(1024 * 1024 * 5)
      val isSample=hbaseTable.equalsIgnoreCase("HEAT_1S")

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
        if (list.size() > 5) {
          mutator.mutate(list)
          mutator.flush()
          logger.info("----------------------flush 5 puts----------------------")
          list.clear()
        }
        putCount=putCount+1
      }
      mutator.mutate(list)
      mutator.flush()
      logger.info("----------------"+hbaseTable+"------flush all puts count: "+putCount+" --------------------------")
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
    val checkpointPath = prop.getProperty("spark.checkpointPath","/tmp/streaming/checkpoint/heat1s")
    val ssc = StreamingContext.getOrCreate(checkpointPath,
      () => {
        createContext(prop)
      })
    ssc.start()
    ssc.awaitTermination()
  }
}
