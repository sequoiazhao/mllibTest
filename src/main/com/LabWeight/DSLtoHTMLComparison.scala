package com.LabWeight

import com.jayway.jsonpath.JsonPath
import net.minidev.json.JSONArray

import scalaj.http.{Http, HttpOptions}

/**
  * @author zhaoming on 2018-07-11 15:25
  **/
object DSLtoHTMLComparison {

  def main(args: Array[String]): Unit = {

    val searchDSL =
      """
        |{
        |  "from": "0",
        |  "size": "30",
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "query_string": {
        |            "query": "(deleted:0 AND tencent_online:1) OR (starttime:[* TO 1531275120000] AND endtime:[1531275120000 TO *] AND _type:vod_live)"
        |          }
        |        }
        |,
        |        {
        |          "query_string": {
        |            "query": "category_name:电影 AND -tag:电影解说"
        |          }
        |        }
        |,
        |        {
        |          "query_string": {
        |        	"query": "(_type:vod AND search_platform:(tv)) OR (_type:vod_live) OR (_type:vod_carousel_topic) OR (_type:vod_special_hotword)"
        |          }
        |        }
        |
        |      ]
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "objid",
        |      "objname",
        |      "objtype",
        |      "category_name",
        |      "available",
        |      "pubdate",
        |      "obj_hot_score",
        |      "play_hotness",
        |      "search_hotness",
        |	  "poster"
        |    ]
        |  }
        |,
        |  "sort":[
        |    {
        |        "release_year":{
        |            "order":"desc"
        |        }
        |    }
        |  ]
        |}
      """.stripMargin


    val httpStr = "http://10.18.210.224:9214/vod_online/_search"

    val result = Http(httpStr)
      .postData(searchDSL)
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .option(HttpOptions.readTimeout(100000)).asString

    println(result)

    val json = JsonPath.parse(result.body)

    val resultName: JSONArray = json.read("$.hits.hits[*]._source.objname")
    val resultId: JSONArray = json.read("$.hits.hits[*]._source.objid")
    val resultPic: JSONArray = json.read("$.hits.hits[*]._source.poster")


    val searchDSLOnline =
      """
        |{
        |	"from": "0",
        |	"size": "30",
        |	"query": {
        |		"bool": {
        |			"filter": [{
        |				"query_string": {
        |					"query": "(deleted:0 AND tencent_online:1) OR (starttime:[* TO 1531275120000] AND endtime:[1531275120000 TO *] AND _type:vod_live)"
        |				}
        |			},
        |			{
        |				"query_string": {
        |					"query": "category_name:电影 AND -tag:电影解说"
        |				}
        |			},
        |			{
        |				"query_string": {
        |					"query": "(_type:vod AND search_platform:(tv)) OR (_type:vod_live) OR (_type:vod_carousel_topic) OR (_type:vod_special_hotword)"
        |				}
        |			}],
        |			"should": {
        |				"function_score": {
        |					"script_score" : {
        |                "script" : {
        |                  "source": "if(doc['available'].value == 1011 || doc['available'].value == 1016 || doc['available'].value == 1017|| doc['available'].value == 1018){ return doc['pubdate_weight'].value*0.6 + doc['obj_hot_score'].value*0.4+ 2}else{ return doc['pubdate_weight'].value*0.6 + doc['obj_hot_score'].value*0.4}"
        |                }
        |            }
        |				}
        |			}
        |		}
        |	},
        |	"_source": {
        |		"includes": ["objid",
        |		"objname",
        |		"objtype",
        |		"category_name",
        |		"available",
        |		"available",
        |		"pubdate",
        |		"obj_hot_score",
        |		"play_hotness",
        |		"search_hotness",
        |		"poster"]
        |	}
        |}
      """.stripMargin


    val resultOnline = Http(httpStr)
      .postData(searchDSLOnline)
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .option(HttpOptions.readTimeout(100000)).asString

    println(resultOnline)


    val jsonOnline = JsonPath.parse(resultOnline.body)

    val resultNameOnline: JSONArray = jsonOnline.read("$.hits.hits[*]._source.objname")
    val resultIdOnline: JSONArray = jsonOnline.read("$.hits.hits[*]._source.objid")
    val resultPicOnline: JSONArray = jsonOnline.read("$.hits.hits[*]._source.poster")


    var content: String = ""
    resultName.toArray.zipWithIndex.foreach { x =>
      //println(resultId.get(x._2), x._1)
      content = content + "<tr><td align='center' width=240>" +
        "<img  width=240 height=135 src=\"" + resultPic.get(x._2) + "\"/></br><b>" + x._1 + "</b></td><td  align='center' width=240>" +
        "<img  width=240 height=135 src=\"" + resultPicOnline.get(x._2) + "\"/></br><b>" + resultNameOnline.get(x._2) + "</b></td><tr>"
    }
    println(content)

    val title = "腾讯牌照—最新电影"

    val htmlHeadString = "<!DOCTYPE html><html lang='zh-cn'><head><meta charset='utf-8'/><title>" + title + "</title></head><body>"
    val endString = "</body>"
    val table = "<table border=\"1\" cellspacing=\"3\" align=\"center\">" +
      "<tr bgcolor='94abff'><td colspan='2' align='center'><h2>" + title + "</h2></td></tr>" +
      "<tr bgcolor='94abff'><td align='center'>按出品时间排序</td><td align='center'>按出品时间+热度+内容提供商排序</td></tr>"
    val tablend = "</table>"
    println(htmlHeadString + table + content + tablend + endString)

  }

}
