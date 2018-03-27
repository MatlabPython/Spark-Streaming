package com.gsafety.lifeline.bigdata.streaming.bridge

import java.text.SimpleDateFormat
import java.util
import java.util._

import com.gsafety.lifeline.bigdata.pojo.Bridge

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
/**
  * Created by hadoop on 2017/9/21.
  */
object Test {
  def main(args: Array[String]): Unit = {
    /*val simpleFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    val aaa = 1505988755906L
    println(simpleFormat.format(aaa))
val sss = new Bridge();
    println(sss.level)
sss.level = 90
    sss.values.append(0.23F)
    println(sss.values)*/
    val gitgit = "试试git"
    val aaa = mutable.Buffer[Float]()
    aaa.append(0.987F)
    aaa.append(0.87F)
    util.Arrays.toString(aaa.toArray)
    aaa.mkString(",")
    val sdsd : util.List[java.lang.Float] = aaa.map(x => java.lang.Float.valueOf(x))
    println(aaa.mkString(","))
    println(sdsd)
  }
}
