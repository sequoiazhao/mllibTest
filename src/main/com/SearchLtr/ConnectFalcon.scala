package com.SearchLtr

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.sql.functions._
import org.joda.time.DateTime

import scalaj.http.{Http, HttpOptions}

/**
  * @author zhaoming on 2018-09-18 10:44
  **/
object ConnectFalcon {

  def main(args: Array[String]): Unit = {
//    val result = Http("http://123.103.124.17:8080/dass/ReportServer?op=fs_load&cmd=fs_signin&_=1501493370707")
//      .header("Content-Type", "application/json")
//      .header("Charset", "UTF-8")
//      .option(HttpOptions.readTimeout(100000)).asString

//    val warningStr=
//      """
//        [{"endpoint": "dass-realtime-monitor-new", "metric": "dassrealtimestatenew", "timestamp": 1537239292, "step": 60, "counterType": "GAUGE", "tags": "", "value": 1}]
//      """.stripMargin
//
//    val result = Http("http://192.168.17.13:1988/v1/push")
//      .postData(warningStr)
//      .header("Content-Type", "application/json")
//      .header("Charset", "UTF-8")
//      .option(HttpOptions.readTimeout(100000)).asString
//
//    println(result)

    val loc = new Locale("en")
    val fromTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", loc)
    //dTime = fromTime.parse(pubTime).getTime / 100000000000.0

    val nowToday = fromTime.parse(DateTime.now.toString("yyyy-MM-dd HH:mm:ss")).getTime
    println(nowToday/1000)
  }


}
