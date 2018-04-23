package com.SearchLtr

import com.jayway.jsonpath.JsonPath
import net.minidev.json.JSONArray
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.json4s.jackson.JsonMethods.parse

import scalaj.http.{Http, HttpOptions}

/**
  * @author zhaoming on 2018-04-12 9:22
  **/
object HttpBackTest {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val MyContext = new HiveContext(sc)

    val mdd = sc.textFile("D:\\dataframeData\\local01")

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val datardd = mdd.map { x =>
      val s1 = x.replace("[", "").replace("]", "").split(",")
      (s1(0), s1(1), s1(2), s1(3), s1(4), s1(5), s1(6), s1(7), s1(8), s1(9), s1(10))
    }
    val relDataAll = datardd.toDF("score", "keyid", "rank", "mediaid", "lengthweight", "logplaytimes", "logsearchtimes", "new", "fee", "categoryweight", "srcsearchkey")
      .persist(StorageLevel.MEMORY_ONLY_SER)

    relDataAll.select("srcsearchkey", "mediaid", "score").show(relDataAll.count().toInt, false)


    val tdd = sc.textFile("D:\\dataframeData\\local02")

    // tdd.foreach(println)

    val relData = tdd.map { x =>
      val s1 = x.replace("[", "").replace("]", "").replace(", ", ":").split(",")
      (s1(0), s1(1))
    }.toDF("srcsearchkey", "mediaid")

    val data = relData.rdd.take(relData.count.toInt).map { mex =>
      val searchx =
        """{
            "query": {
              "bool": {
                "filter": [
                  {
                    "terms": {
                      "_id": """ + mex.get(1).toString.replace("WrappedArray(", "[\"").replace(")", "\"]").replace(":", "\",\"") +
          """
                    }
                  },
                  {
                    "sltr": {
                      "_name": "logged_featureset",
                      "featureset": "unionsearch_vod_similarity_features",
                      "params": {
                        "keywords": """ + "\"" + mex.get(0).toString + "\"" +
          """
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
                  "named_query": "logged_featureset"
                }
              }
            },
           "_source": {
 	             "includes": ["log_entry1",	"objname"]
            }
          }""".stripMargin


      val result = Http("http://10.18.210.224:9600/unionsearch_vod_online/_search?size=100")
        .postData(searchx)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(100000)).asString

      //println(result)
      //println("show")
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
        (mex.get(0).toString, nameArray(x._2).toString, x._1.toString, LogValueArray(x._2).toString.toDouble * 10)
      }


      resultValue.sortBy(_._4)(Ordering.Double.reverse).foreach(println)
      resultValue
    }

  }

}


//    val searchx =
//      """
//        {
//                    "query": {
//                      "bool": {
//                        "filter": [
//                          {
//                            "terms": {
//                              "_id": ["1845873","3876623","11012293372","2298651","11013548160","11013392721","1572316","1974267","2150681","1334997","282136","11012802342","359824","11013775192","11012303645","2191506","2290858","1910440","2277207","2049053","3720904","11013392722","11013451305","1759499","2279666","358783","1973765","1365692","1565505","1332395","1481615","11012742695","11013858100","11013876067","1717808","965431","11013316672","2184061","1568916","11013799259","11013392723","2145511","11013914889","965382","1771908","358035","2149257","357948","358018","11013691105","2146044","11013580395","11013954280","11012990271","2184051","1483827","2222361","2225028","11013563033","1334706","359726","11012802441","11013835873","570662","282478","11013392719","11013547800","282383","2173782","2243416","3369969","11012925070","3835627","1584867","2149254","460355","2178481","3734606","11012831504"]
//                            }
//                          },
//                          {
//                            "sltr": {
//                              "_name": "logged_featureset",
//                              "featureset": "unionsearch_vod_similarity_features",
//                              "params": {
//                                "keywords": "BX"
//                              }
//                            }
//                          }
//                        ]
//                      }
//                    },
//                    "ext": {
//                      "ltr_log": {
//                        "log_specs": {
//                          "name": "log_entry1",
//                          "named_query": "logged_featureset"
//                        }
//                      }
//                    },
//                    "_source": {
//        		"includes": ["log_entry1",	"objname"]
//                  }
//        }
//      """.stripMargin


//    relData.show(1000, false)
//        relData.filter(col("srcsearchkey").===(lit("BX"))).show(100, false)
//        relData.filter(col("srcsearchkey").===(lit("XC"))).show(100, false)
//    relData.filter(col("srcsearchkey").===(lit("CFS"))).show(100, false)

