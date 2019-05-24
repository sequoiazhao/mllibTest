package com.LabWeight

import java.io.{BufferedInputStream, FileInputStream, InputStreamReader}
import java.util.zip.GZIPInputStream

import com.jayway.jsonpath.JsonPath
import jdk.nashorn.internal.parser.JSONParser
import net.minidev.json.JSONArray
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scalaj.http.{Http, HttpOptions}

/**
  * @author zhaoming on 2018-05-16 16:22
  **/
object LocalParseResultXonline {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val MyContext = new HiveContext(sc)


    val singleTag = Array(Seq("科幻"), Seq("喜剧"), Seq("动画"), Seq("犯罪"), Seq("爱情"), Seq("青春"), Seq("灾难"), Seq("家庭"), Seq("枪战"), Seq("魔幻"), Seq("悬疑"), Seq("奇幻")
      , Seq("战争"), Seq("悲剧"), Seq("欧洲"), Seq("恐怖"), Seq("惊悚"))
    //Seq("恐怖", "惊悚"), Seq("动作", "运动", "冒险", "枪战"), Seq("巨制", "好莱坞"), Seq("悲剧", "催泪"), Seq("灾难", "空难", "逃亡", "野外生存", "地震"))




    val lines = scala.io.Source.fromFile("d://data/result1.txt").mkString

    val json = JsonPath.parse(lines)


    val resultName: JSONArray = json.read("$.data[*].objname")
   resultName.toArray.foreach(println)
//    val allstr = singleTag.map { x =>
//      var str: String = null
//      if (x.length == 1) {
//        str = "\"" + x.mkString + "\""
//      } else {
//        str = "\"" + x.map { y =>
//          "taga.keyword:" + y.mkString
//        }.mkString(" OR ")
//      }
//
//      println(str)
//
//
//val  searchStr="http://10.18.210.224:9210/_api/v1.2/_filter?q=category_name:电影&from=0&size=100&licence=wasu&sequence=1502878050849&subscriber_id=13288344&index=9&device_id=86100300900000100000060c12345678&package_name=com.jamdeo.tv.vod&package_version=1000000855&rs_type=ss&rs_version=1.4&resource=vod&rkey=uid&feature_code=86100300900000100000060c&model_id=0&sort=hot&desc"
//
//      val result = Http(searchStr)
//        .header("Content-Type", "application/json")
//        .header("Content-encoding","gzip")
//        .header("Charset", "UTF-8")
//        .option(HttpOptions.readTimeout(100000)).asString
//
//     //val ss =   new GZIPInputStream(new BufferedInputStream(new FileInputStream(result.body)))
//
//
//
//      println(result)
//      //GZIPInputStream gzipIn = new GZIPInputStream(result)
//
////      println(result.code)
////      println(result.body)
//
//   //   val json = JsonPath.parse(new InputStreamReader(new GZIPInputStream())
//
//     // val resultId: JSONArray = json.read("$.searchResultList[*].mediaId")
//   //   val resultName: JSONArray = json.read("$.searchResultList[*].mediaName")
//
//    }
  }
}

