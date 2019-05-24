package com.LabWeight

import com.jayway.jsonpath.JsonPath
import net.minidev.json.JSONArray
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scalaj.http.{Http, HttpOptions}

/**
  * @author zhaoming on 2018-05-16 16:22
  **/
object LocalParseResultX {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val MyContext = new HiveContext(sc)


    val singleTag = Array(Seq("科幻"), Seq("喜剧"), Seq("动画"), Seq("犯罪"), Seq("爱情"), Seq("青春"), Seq("灾难"), Seq("家庭"), Seq("枪战"), Seq("魔幻"), Seq("悬疑"), Seq("奇幻")
      , Seq("战争"), Seq("悲剧"), Seq("欧洲"), Seq("恐怖"), Seq("惊悚"), Seq("动作"))
    //Seq("恐怖", "惊悚"), Seq("动作", "运动", "冒险", "枪战"), Seq("巨制", "好莱坞"), Seq("悲剧", "催泪"), Seq("灾难", "空难", "逃亡", "野外生存", "地震"))

    val allstr = singleTag.map { x =>
      var str: String = null
      var str2: String = null
      var str3:String = null
      if (x.length == 1) {
        str = "\"" + x.mkString + "\""
        str2 =  x.mkString
        str3 =  x.mkString+"片"
      } else {
        str = "\"" + x.map { y =>
          "taga.keyword:" + y.mkString
        }.mkString(" OR ")
      }

      println(str)
      println(str2)

      val searchDSL =
        """
        {
          "query": {
            "bool": {
              "must": [{
      	"term":{"category_name":"电影"}
       },
                {
                  "nested": {
                    "path": "tag_score",
                    "query": {
                      "function_score": {
                        "query": {
                          "term": {
                            "tag_score.name": """ + str +
          """
                          }
                        },
                        "script_score": {
                          "script": "doc['tag_score.value'].value"
                        }
                      }
                    }
                  }
                }
              ]
            }
          },
          "_source": {
            "includes": [
              "objid",
              "objname",
              "category_name",
              "poster"
            ]
          },
          "size": 100
        }
      """.stripMargin
      // println(searchDSL)

      val searchDSLOnline =
        """{
            "from": "0",
            "size": "100",
            "query": {
              "bool": {
                "filter": [
                  {
                    "query_string": {
                      "query": "(tag:"""+str2+""" OR tag:"""+str3+""") AND category_name:电影"
                    }
                  }
                ]
              }
            },
            "_source": {
              "includes": [
                "objid",
                "objname",
                |"poster"
              ]
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
            "aggregations": {
              "category_id": {
                "terms": {
                  "field": "category_id",
                  "size": 100,
                  "order": {
                    "_count": "desc"
                  }
                }
              }
            }
          }
        """.stripMargin

      val httpStr = "http://10.18.210.224:9210/vod_online/vod/_search"

      val result = Http(httpStr)
        .postData(searchDSL)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(100000)).asString

      val resultOnline = Http(httpStr)
        .postData(searchDSLOnline)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(100000)).asString


      println(result)
      println(resultOnline)

      val json = JsonPath.parse(result.body)
      val jsonOnline = JsonPath.parse(resultOnline.body)


      val resultName: JSONArray = json.read("$.hits.hits[*]._source.objname")
      val resultPic:JSONArray=json.read("$.hits.hits[*]._source.poster")

      val NameWithPic = resultName.toArray.zipWithIndex.map{x=>
        "<img  width=120 height=75 src=\""+resultPic.get(x._2)+"\"/>"+ x._1.toString+"</br>"
         //x._1.toString+"</br>"
      }.mkString

      val resultNameOnline: JSONArray = jsonOnline.read("$.hits.hits[*]._source.objname")
      val resultPicOnline:JSONArray=jsonOnline.read("$.hits.hits[*]._source.poster")

      val NameWithPicOnline = resultNameOnline.toArray.zipWithIndex.map{x=>
        "<img  width=120 height=75 src=\""+resultPicOnline.get(x._2)+"\"/>"+ x._1.toString+"</br>"
        //x._1.toString+"</br>"
      }.mkString


      "<tr><td  width='200px' valign='top'>" + x.mkString(",") + "</td><td>" + NameWithPic+ "</td><td>" +NameWithPicOnline + "</td></tr>"

    }.mkString

    val htmlHeadString = "<!DOCTYPE html><html lang='zh-cn'><head><meta charset='utf-8'/></head><body>"
    val endString = "</body>"
    val table = "<table border=\"1\" cellspacing=\"1\" align=\"center\"><tr bgcolor='94abff'><td>搜索关键词</td><td>nested结果</td><td>线上结果</td></tr>"
    val tablend = "</table>"
    println(htmlHeadString + table + allstr + tablend + endString)
  }

}
