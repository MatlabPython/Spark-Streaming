package com.gsafety.lifeline.bigdata.hive2hbase


import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date, Properties}

import com.gsafety.lifeline.bigdata.hbase.HBaseConnection
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{BufferedMutatorParams, Put}
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable
/**
  * Created with Intellij IDEA.
  * User: naichun
  * Date: 2017-11-03
  * Time: 19:44
  */
object GasWater {
  def main(args: Array[String]): Unit = {
    val sparkconf =new SparkConf().setAppName("Hive2Hbase")
    val sc =new SparkContext(sparkconf)
    val sqlContext = new HiveContext(sc)

    val conf = args(0)
    val prop = new Properties()
    val file =new FileInputStream(conf)
    prop.load(file)
    file.close()
    if(!prop.containsKey("zookeeperQuorum") || !prop.containsKey("hbaseTable") || !prop.containsKey("hiveTable")){
      println("！！！！缺少配置项！！！")
      System.exit(1)
    }
    val cf="M"
    val zookeeperPort="2181"
    var sampleTime=1000
    if(prop.containsKey("sampleTime")){
      sampleTime=prop.getProperty("sampleTime").toInt
    }
    val hbaseTable=prop.getProperty("hbaseTable")
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
    for( hiveTable <- hiveTables){
      while(start.compareTo(end) <= 0) {
        val dt=simpleFormat.format(start.getTime)
        println("--------------------" + hiveTable+" dt="+dt + "-----------------------")
        val result=sample(sqlContext,hiveTable,dt,sampleTime)
        result.foreachPartition(x=>{
          insertBatchHBase(cf,hbaseTable,x,zookeeperPort,zookeeperQuorum)
        })
        start.set(Calendar.DATE, start.get(Calendar.DATE) + 1);
      }
    }
    sc.stop()
  }

  def sample(sqlContext:HiveContext,hiveTable:String,dt:String,sampleTime:Int):RDD[Data] ={
    val hiveResult: DataFrame = sqlContext.sql("select * from "+hiveTable+" where dt= "+dt)
    val mapResult: RDD[(String, Data)] = hiveResult.map(x => {
      val timeKey =x.getLong(8)/sampleTime
      val key =x.getString(0)+"_"+x.getString(1)+"_"+x.getString(2)+"_"+timeKey
      val data=Data(x.getString(0),x.getString(1),x.getString(2),
        x.getLong(3),x.getString(4),
        x.getString(5),x.getString(6),
        x.getLong(8),x.getInt(9),
        x.getList[Float](10))
      (key , data)
    })
    val os: RDD[(String, Data)] = mapResult.reduceByKey((data1: Data, data2: Data) => {
      if (data2.time <= data1.time) {
        data2
      } else {
        data1
      }
    })
    val results: RDD[Data] =os.map(x => x._2)
    results
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

        if (list.size() > 1000) {
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

  def ArraytoString(array: mutable.Buffer[Float]): String = {
    val valuesString = array.mkString(",")
    valuesString
  }
  case class Data (location: String,
                   terminal: String,
                   sensor: String,
                   rtime: Long,
                   sensorType:String,
                   dataType:String,
                   monitoring:String,
                   time:Long,
                   level: Int,
                   values: mutable.Buffer[Float]
                  )

}
