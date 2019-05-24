package com.LabWeight

import com.jayway.jsonpath.JsonPath
import net.minidev.json.JSONArray
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

import scalaj.http.{Http, HttpOptions}

/**
  * @author zhaoming on 2018-05-16 16:22
  **/
object LocalParseResult {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val MyContext = new HiveContext(sc)


    val singleTag = Array(Seq("科幻"), Seq("喜剧"), Seq("动画"), Seq("犯罪"), Seq("爱情"), Seq("青春"), Seq("灾难"), Seq("家庭"), Seq("枪战"), Seq("魔幻"), Seq("悬疑"), Seq("奇幻")
     , Seq("战争"), Seq("悲剧"), Seq("欧洲"), Seq("恐怖"), Seq("惊悚"),
      Seq("恐怖", "惊悚"), Seq("动作", "运动", "冒险", "枪战"), Seq("巨制", "好莱坞"), Seq("悲剧", "催泪"), Seq("灾难", "空难", "逃亡", "野外生存", "地震"))

    val allstr = singleTag.map { x =>
      var str: String = null
      if (x.length == 1) {
        str = "\""+"taga.keyword:" + x.mkString
      } else {
        str ="\""+ x.map { y =>
          "taga.keyword:" + y.mkString
        }.mkString(" OR ")
      }

      //      println(str)
      //      str
      val searchDSL =
      """
        {
          "query": {
          	"bool":{
          	 "filter": [
                {
                  "query_string": {
                    "query": "deleted:0 AND wasu_online:1"
                  }
                }
        ,
                {
                  "query_string": {
                    "query": """ + str +
        """ AND category_name:电影"
                  }
                }

              ]
          	}

          },
          "sort": [
            {
              "is_trailer": {
                "order": "asc"
              }
            },
            {
              "obj_hot_score": {
                "order": "desc"
              }
            }

          ],
          "_source": {
            "includes": [
              "objid",
              "objname",
              "category_name"
            ]
          },
          "size": 100
        }
      """.stripMargin
     // println(searchDSL)

      val httpStr = "http://10.18.210.224:9210/vod_online/vod/_search"

      val result = Http(httpStr)
        .postData(searchDSL)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(100000)).asString

      println(result)


      val json = JsonPath.parse(result.body)


      val resultName: JSONArray = json.read("$.hits.hits[*]._source.objname")


      "<tr><td  width='200px' valign='top'>" + x.mkString(",") + "</td><td>" + resultName.toArray.mkString("</br>") + "</td></tr>"

    }.mkString

    val htmlHeadString = "<!DOCTYPE html><html lang='zh-cn'><head><meta charset='utf-8'/></head><body>"
    val endString = "</body>"
    val table = "<table border=\"1\" cellspacing=\"1\" align=\"center\"><tr bgcolor='94abff'><td>搜索关键词</td><td>结果</td></tr>"
    val tablend = "</table>"
    println(htmlHeadString + table + allstr + tablend + endString)
  }

}
