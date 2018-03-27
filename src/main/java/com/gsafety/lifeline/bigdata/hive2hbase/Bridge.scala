package com.gsafety.lifeline.bigdata.hive2hbase
import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Collections, Date, Properties}

import com.gsafety.lifeline.bigdata.hbase.HBaseConnection
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{BufferedMutatorParams, Put}
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created with Intellij IDEA.
  * User: naichun
  * Date: 2017-11-03
  * Time: 19:44
  */
object Bridge {
  case class Data (location: String,
                   terminal: String,
                   sensor: String,
                   rtime: Long,
                   sensorType:String,
                   dataType:String,
                   monitoring:String,
                   time:Long,
                   level: Int,
                   values: mutable.Buffer[java.lang.Float]
                  )
  def main(args: Array[String]): Unit = {
    val sparkconf =new SparkConf().setAppName("Hive2Hbase")
//    sparkconf.set("spark.default.parallelism", "6")
    val sc =new SparkContext(sparkconf)
    val sqlContext = new HiveContext(sc)

    val conf = args(0)
    val prop = new Properties()
    val file =new FileInputStream(conf)
    prop.load(file)
    file.close()
    if(!prop.containsKey("zookeeperQuorum") || !prop.containsKey("hiveTable")){
      println("！！！！缺少配置项！！！")
      System.exit(1)
    }
    val cf="M"
    val zookeeperPort="2181"
    var sampleTime=1000
    if(prop.containsKey("sampleTime")){
      sampleTime=prop.getProperty("sampleTime").toInt
    }
    val hiveTables=prop.getProperty("hiveTable").split(",")
    val zookeeperQuorum=prop.getProperty("zookeeperQuorum")
    val simpleFormat = new SimpleDateFormat("yyyyMMdd")
    var startDay=new Date()
    var endDay=new Date()
    if(prop.containsKey("startDay") && prop.containsKey("endDay") ){
      startDay= simpleFormat.parse(prop.getProperty("startDay"))
      endDay= simpleFormat.parse(prop.getProperty("endDay"))
    }
    val start=Calendar.getInstance()
    start.setTime(startDay)
    val end= Calendar.getInstance()
    end.setTime(endDay)
      while (start.compareTo(end) <= 0) {
        for( hiveTable <- hiveTables) {
        val dt = simpleFormat.format(start.getTime)
        println("--------------------" + hiveTable+" dt="+dt + "-----------------------")
          if("false".equalsIgnoreCase(prop.getProperty("isTest"))){
            val hiveResult: DataFrame = sqlContext.sql("select * from " + hiveTable + " where dt= " + dt)
            val test =hiveResult.map(row=>{
              row(0)
            })
            val datas: RDD[Data] = hiveResult.map(x => {
              val data = Data(x.getString(0), x.getString(1), x.getString(2),
                x.getLong(3), x.getString(4),
                x.getString(5), x.getString(6),
                x.getLong(8), x.getInt(9),
                x.getList[java.lang.Float](10))
              data
            })
//            datas.persist(StorageLevel.MEMORY_AND_DISK)
            /**
              * 明细数据
              */
            //        datas.foreachPartition(x => {
            //          insertBridgeHBase(cf, "", x, zookeeperPort, zookeeperQuorum)
            //        })
            /**
              * 秒采样数据
              */
            val result_1S = sample(datas, 1000)
            result_1S.foreachPartition(x => {
              insertBridgeHBase(cf, "1S", x, zookeeperPort, zookeeperQuorum)
            })
            /**
              * 10分钟采样数据
              */
            val result_10M = sample(datas, 600000)
            result_10M.foreachPartition(x => {
              insertBatchHBase(cf, "BRIDGE_TENDENCY_10M", x, zookeeperPort, zookeeperQuorum)
            })
            /**
              * 10分钟特征数据
              */
            val result_S10M = datas.filter(data => data.sensorType == "S10M")
            result_S10M.foreachPartition(x => {
              insertBatchHBase(cf, "BRIDGE_FEATURE_10M", x, zookeeperPort, zookeeperQuorum)
            })
//            datas.unpersist()
          }
          }
        start.set(Calendar.DATE, start.get(Calendar.DATE) + 1);
    }
    sc.stop()
  }

  def sample(datas: RDD[Data],sampleTime:Int):RDD[Data] ={
    val mapResult= datas.map(x => {
      //对取样数据求除数
      val timeKey = x.time / sampleTime;
      val key = x.location + "_" + x.terminal + "_" + x.sensor + "_" + timeKey
      (key, x)
    })
    val result: RDD[Data] =  mapResult.groupByKey().map(x => {
      val count = x._2.size
      val data  = x._2.toList
      var zong = mutable.Buffer[java.lang.Float]()
      var whichData = 0
      //先判断有没有警报，有报警的报警优先
      for (va <- 0 until data.size -1 ){
        if(data(va).level <= data(va+1).level){
          whichData = va+1
        }else{
          whichData = va
        }
      }
      //求均值
      if(data(whichData).values.size() == 1){
        for(clo <- 0 until data(whichData).values.size()){
          val maxAndMin = new ListBuffer[java.lang.Float]
          var sum = 0.0
          for(row <-0 until data.size){
            val a: Float =data(row).values(clo)
            maxAndMin.append(data(row).values(clo))
            sum += data(row).values.get(clo)
          }
          zong.append(Collections.max(maxAndMin))
          zong.append(Collections.min(maxAndMin))
          zong.append((sum/count).toFloat)
        }
      }else{
        zong = data(whichData).values
      }
      val dataResult = Data(data(whichData).location,data(whichData).terminal,data(whichData).sensor,
        data(whichData).rtime,data(whichData).sensorType,
        data(whichData).dataType,data(whichData).monitoring,
        data(whichData).time,data(whichData).level,
        zong)
      dataResult//返回集合
    })
    result
  }


  def insertBridgeHBase(cf: String, sampleType: String, recordPartitions: Iterator[Data], hbaseZookeeperPort: String, zookeeperquorum: String): Unit = {

    try {
      var connection = HBaseConnection.getConnection
      if (connection == null) {
        HBaseConnection.createConnecion(hbaseZookeeperPort, zookeeperquorum)
        connection = HBaseConnection.getConnection
      }

      val map = new util.concurrent.ConcurrentHashMap[String, util.ArrayList[Put]]()
      val simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
      val simpleFormats = new SimpleDateFormat("ss");
      for (data <- recordPartitions) {
        val ternal = data.terminal
        val sensor = data.sensor
        val time = data.time
        val location = data.location
        val secondss = simpleFormats.format(time)
        val md5String = sensor + ternal
        var tableNameStr = ""
        val valueString = ArraytoString(data.values)
        if (sampleType.equals("") || sampleType == null) {
          tableNameStr = "BRIDGE" + "_" + ternal
        } else {
          tableNameStr = "BRIDGE" + "_" + ternal + "_" + sampleType
        }
        val tableName = TableName.valueOf(tableNameStr)
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
        put.addColumn(cf.getBytes, "values".getBytes, Bytes.toBytes(valueString))

        val key = tableName.getNameAsString;


        var list = map.get(key)

        if (list == null) {
          list = new util.ArrayList[Put]()
          list.add(put)
          map.put(key, list)
        } else {
          if (list.size() > 500) {
            val params = new BufferedMutatorParams(tableName);
            val mutator = connection.getBufferedMutator(params);
            mutator.mutate(list)
            mutator.flush()
            mutator.close()
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
        mutator.close()
      }
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
  }
  def insertBatchHBase(cf: String, hbaseTable: String, recordPartitions: Iterator[Data], hbaseZookeeperPort: String, zookeeperquorum: String): Unit = {


    try {

      var connection = HBaseConnection.getConnection

      if (connection == null) {

        HBaseConnection.createConnecion(hbaseZookeeperPort, zookeeperquorum)
        connection = HBaseConnection.getConnection
      }

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
        val seconds = simpleFormats.format(time)
        val md5String = sensor + ternal

        val startrowkey = MD5Hash.getMD5AsHex(md5String.getBytes("utf-8"))

        val rowkey = startrowkey.substring(0, 6) + ":" + ternal + ":" + sensor + ":" + (Long.MaxValue - time)

        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(cf.getBytes, "location".getBytes, Bytes.toBytes(location))
        put.addColumn(cf.getBytes, "seconds".getBytes, Bytes.toBytes(seconds))
        put.addColumn(cf.getBytes, "time".getBytes, Bytes.toBytes(simpleFormat.format(new Date(time))))
        put.addColumn(cf.getBytes, "rtime".getBytes, Bytes.toBytes(data.rtime))
        put.addColumn(cf.getBytes, "sensorType".getBytes, Bytes.toBytes(data.sensorType))
        put.addColumn(cf.getBytes, "dataType".getBytes, Bytes.toBytes(data.dataType))
        put.addColumn(cf.getBytes, "monitoring".getBytes, Bytes.toBytes(data.monitoring))
        put.addColumn(cf.getBytes, "level".getBytes, Bytes.toBytes(data.level))

        val valueString = ArraytoString(data.values)

        put.addColumn(cf.getBytes, "values".getBytes, Bytes.toBytes(valueString))

        list.add(put)

        if (list.size() > 500) {
          mutator.mutate(list)
          list.clear()
        }

        putCount = putCount + 1;
      }
      mutator.mutate(list)
      mutator.flush()
      mutator.close()

    } catch {
      case ex: Exception => ex.printStackTrace()
    }
  }

  def ArraytoString(array: mutable.Buffer[java.lang.Float]): String = {
    val valuesString = array.mkString(",")
    valuesString
  }
}
