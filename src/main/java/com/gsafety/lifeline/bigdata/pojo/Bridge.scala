package com.gsafety.lifeline.bigdata.pojo

import scala.collection.mutable

/**
  * Created by hadoop on 2017/9/21.
  */
class Bridge(){

  var location :String = ""
  var terminal:String = ""
  var sensor:String =""
  var rtime: Long = 0L
  var sensorType :String = ""
  var dataType:String =""
  var monitoring :String = ""
  var time :Long = 0L
  var level : Int = 0
  var values: mutable.Buffer[Float] =mutable.Buffer[Float]()
def Bridge(location :String,
           terminal:String,
           sensor:String,
           rtime: Long,
           sensorType :String,
           dataType:String,
           monitoring :String,
           time :Long,level : Int,
           values: mutable.Buffer[Float]){
  this.location = location
  this.terminal = terminal
  this.sensor =sensor
  this.rtime = rtime
  this.sensorType = sensorType
  this.dataType = dataType
  this.monitoring = monitoring
  this.time = time
  this.level= level
  this.values = values
}
}

