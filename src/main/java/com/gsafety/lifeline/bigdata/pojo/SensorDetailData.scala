package com.gsafety.lifeline.bigdata.pojo

import scala.collection.mutable

/**
  * Created with Intellij IDEA.
  * User: naichun
  * Date: 2017-11-08
  * Time: 13:04
  */
case class SensorDetailData(location: String,
                            terminal: String,
                            sensor: String,
                            rtime: Long,
                            sensorType: String,
                            dataType: String,
                            monitoring: String,
                            time: Long,
                            level: Int,
                            values: mutable.Buffer[Float]
                           )