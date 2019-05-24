package com.LabWeight

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.sql.functions._
import org.joda.time.DateTime

import scalaj.http.{Http, HttpOptions}

/**
  * @author zhaoming on 2018-09-01 9:26
  **/
object InterfaceToHtml {

  def main(args: Array[String]): Unit = {

    val ss = Map("dd" -> 1.0, "ss" -> 2.0)

    val tt = ss.map(x => x._2)

    tt.foreach(println)

    val pubTime = "2018-10-11 12:00:00"

    val pptime = "2015-12-21 12:00:00"

    val loc = new Locale("en")
    val fromTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", loc)
    val dTime = fromTime.parse(pubTime).getTime / 100000000000.0

    val te1 = fromTime.parse(pubTime).getTime

    val te2 = fromTime.parse(pptime).getTime

    println(te1)

    val sds =  DateTime.now.toString("yyyy-MM-dd HH:mm:ss")

     val now = fromTime.parse(sds).getTime
    //    Long startM = sdf.parse(startDate).getTime();
    //            Long endM = sdf.parse(endDate).getTime();
    //            long result = (endM - startM) / (24 * 60 * 60 * 1000) + 1;
    //            System.out.println("起止日期相差:" + result + "天");

    val result =((now- te2) /(24*60*60*1000)+1)/10+150

    println(pptime.substring(0,4))
    println(pptime.substring(0,4).toDouble/result)

    val result2 =((now- te1) /(24*60*60*1000)+1)/10+150



    println(result2)
    println(pubTime.substring(0,4).toDouble/result2)



    //println(dT1)

//    val pptime2 = "1985-01-01 00:00:00"
//
//    println( fromTime.parse(pptime2).getTime)

   // println(dTime * pubTime(3))


    val conList=List("恐怖","惊悚","喜剧","爱情")

    val  strategy1 = List("恐怖","惊悚","喜剧")


  println(  conList.intersect(strategy1).size)

  }

}
