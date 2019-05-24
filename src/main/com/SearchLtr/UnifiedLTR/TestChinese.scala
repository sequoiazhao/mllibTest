package com.SearchLtr.UnifiedLTR

import org.apache.spark.sql.functions._

import scalaj.http.{Http, HttpOptions}

/**
  * @author zhaoming on 2018-07-23 18:12
  **/
object TestChinese {

  def main(args: Array[String]): Unit = {
    val testTitle="琅琊榜|[\"objname\",\"other_name\"]"
    val testStr ="[11012397772, 11012940880, 11013139326, 11013015488, 11012439799, 11015714726, 11012940881, 11013360463, 1751208, 11013090366, 11015813955, 11012397806, 11012940882, 1972270, 11012397801, 11013143867, 11013235186, 11013090367, 11012397807, 11013292217, 11012397787, 11013235187, 11013137987, 11015816598, 1574747, 3734567, 11013015487, 11013498312, 11013495335, 11015466509, 11012411400]"

    val searchx =
      """{
            "query": {
              "bool": {
                "filter": [
                  {
                    "terms": {
                      "_id": """ + testStr.replace("WrappedArray(", "[\"").replace(")", "\"]").replace(":", "\",\"") +
        """
                    }
                  },
                  {
                    "sltr": {
                      "_name": "logged_featureset",
                      "featureset": "2_1_2_union_app_vod_chinese_exp_similarity",
                      "params": {
                        "keywords": """ + "\"" + testTitle + "\"" +
        """
             | "fields":["objname","other_name"]
                      }
                    }
                  }
                ]
              }
            },
            "ext": {
              "ltr_log": {
                "log_specs": {
                  "name": "log_entry1",
                  "named_query": "logged_featureset",
                  |"missing_as_zero":true
                }
              }
            },
           "_source": {
 	             "includes": ["log_entry1",
               "objname"]
            }
          }""".stripMargin
    val result = Http("http://10.18.210.224:9214/vod_online/_search?size=100")

      .postData(searchx)
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .option(HttpOptions.readTimeout(100000)).asString

    println(result)
  }

}
