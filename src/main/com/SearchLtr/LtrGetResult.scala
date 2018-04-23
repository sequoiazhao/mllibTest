package com.SearchLtr

import com.jayway.jsonpath.JsonPath
import net.minidev.json.JSONArray
import org.apache.spark.sql.functions._
import org.json4s.jackson.JsonMethods.parse

import scalaj.http.{Http, HttpOptions}

/**
  * @author zhaoming on 2018-04-12 16:04
  **/

//"model": "unionsearch_vod_ltr_model_simi_spark"
//unionsearch_vod_ltr_model_simi



object LtrGetResult {
  def main(args: Array[String]): Unit = {
    val testData = Array("LH", "MH", "XC", "XCA", "SG", "DG", "BP", "DL", "ZH", "XCM", "BX")

    val testNewData =Array("HM", "AQ", "CC", "HY", "HL", "NN", "KL", "SD")
    val result = testNewData.map { mex =>
      println(mex)
      val search =
        """
          {
          	"from": "0",
          	"size": "100",
          	"query": {
          		"bool": {
          			"filter": [{
          				"bool": {
          					"should": [{
          						"match_phrase": {
          							"objname.firstletter": {
          								"query": """ + "\"" + mex + "\"" +
          """
          							}
          						}
          					},
          					{
          						"match_phrase_prefix": {
          							"objname.pinyin": {
          								"query": """ + "\"" + mex + "\"" +
          """,
          								"max_expansions": 10
          							}
          						}
          					}]
          				}
          			},
          			{
          				"sltr": {
          					"_name": "logged_featureset",
          					"featureset": "unionsearch_vod_similarity_features",
          					"params": {
          						"keywords": """ + "\"" + mex + "\"" +
          """
          					}
          				}
          			}],
          			"should": {
          				"function_score": {
          					"functions": [{
          						"field_value_factor": {
          							"field": "obj_hot_score",
          							"factor": 1
          						}
          					}]
          				}
          			}
          		}
          	},
          	"_source": {
          		"includes": ["objid",
          		"objname",
          		"play_hotness",
          		"search_hotness",
          		"obj_hot_score"]
          	},
          	"rescore": {
          		"window_size": 1000,
          		"query": {
          			"rescore_query": {
          				"sltr": {
          					"params": {
          						"keywords": """ + "\"" + mex + "\"" +
          """
          					},
          					"model": "unionsearch_vod_ltr_model_simi_spark"
          				}
          			}
          		}
          	},
          	"ext": {
          		"ltr_log": {
          			"log_specs": {
          				"name": "log_entry1",
          				"named_query": "logged_featureset"
          			}
          		}
          	}
          }
        """.stripMargin

      val result = Http("http://10.18.210.224:9214/unionsearch_vod_online/_search?size=100")
        .postData(search)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(100000)).asString

      println(result)

      val json = JsonPath.parse(result.body)
      val resultId: JSONArray = json.read("$.hits.hits[*]._id")
      val resultName: JSONArray = json.read("$.hits.hits[*]._source.objname")
      val resultLog: JSONArray = json.read("$.hits.hits[*].fields._ltrlog[*].log_entry1")

      val idArray = resultId.toArray
      println(idArray.length)
      val logArray = resultLog.toArray
      println(logArray.length)
      val nameArray = resultName.toArray

      val logParse = logArray.map { x =>
        val sx = x.toString match {
          case ax: String => parse(ax)
        }
        sx.values
      }

      val LogValueList = logParse.map { x =>
        val result = x match {
          case sx: List[Map[String, String]] => val sxResult = sx.map {
            ssx =>
              if (ssx.get("value").isEmpty) {
                0.0
              } else {
                ssx("value")
              }
          }
            sxResult
        }
        result

      }

      val LogValueArray = LogValueList.flatten
      //st5.foreach(println)

      val resultValue = idArray.zipWithIndex.map { x =>
        (mex, nameArray(x._2).toString, x._1.toString, LogValueArray(x._2).toString.toDouble * 10)
      }
      resultValue
    }

     result.foreach(x=>x.foreach(println))
  }
}
