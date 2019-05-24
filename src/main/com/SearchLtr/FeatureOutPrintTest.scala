package com.SearchLtr

import com.jayway.jsonpath.JsonPath
import net.minidev.json.JSONArray
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

import scalaj.http.{Http, HttpOptions}

/**
  * @author zhaoming on 2018-06-13 17:26
  **/
object FeatureOutPrintTest {


    def main(args: Array[String]): Unit = {


//          val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
//            .set("spark.sql.warehouse.dir", "/spark-warehouse/")
//          val sc = new SparkContext(conf)
//          val MyContext = new HiveContext(sc)
//
//          val sqlContext = SQLContext.getOrCreate(sc)
//          import sqlContext.implicits._
//
//          val tdd = sc.textFile("D:\\dataframeData\\englocal02_612")
//
//          val relData = tdd.map { x =>
//            val s1 = x.replace("[", "").replace("]", "").replace(", ", ":").split(",")
//            (s1(0), s1(1))
//          }.toDF("srcsearchkey", "mediaid")
//
//      val sss="""[[(11013868671,0.0,1.0), (11013472136,0.0,0.0), (11014470207,0.0,0.0), (11013886696,0.0,0.0), (11013869026,0.0,0.0), (11013740441,0.0,0.0)]]"""
//          val ttt = sss.split("\\), \\(")
//          val stst = ttt.map{x=>
//            val temp =  x.replace("""(""", "").replace(""")""", "")
//              .replace("""[""","").replace("""]""","")
//            val temp2 = temp.split(",")
//            (temp2(0), temp2(1).toDouble, temp2(2).toDouble)
//          }.toList
//
//          val st2 =stst.groupBy(_._1).map { x =>
//            (x._1, x._2.map(_._2).max, x._2.map(_._3).sum).toString
//          }.toArray
//          st2.foreach(println)
//
//          val sss2=Array("""[(11013868671,0.0,1.0), (11013472136,0.0,0.0), (11014470207,0.0,0.0)]""")
//
//          val ttt2 =Array ("""[(11013886696,0.0,0.0), (11013869026,0.0,0.0), (11013740441,0.0,0.0)]""")
//
//          val ttt3 = sss2.union(ttt2)
//          ttt3.mkString(",").split("\\],\\[").mkString(", ").split("\\), \\(").foreach(println)



      //输出英文结果
//          val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
//            .set("spark.sql.warehouse.dir", "/spark-warehouse/")
//          val sc = new SparkContext(conf)
//          val MyContext = new HiveContext(sc)
//
//          val mdd = sc.textFile("D:\\dataframeData\\englocal01_612")
//
//          val sqlContext = SQLContext.getOrCreate(sc)
//          import sqlContext.implicits._
//          val datardd = mdd.map { x =>
//            val s1 = x.replace("[", "").replace("]", "").split(",")
//            (s1(0), s1(1), s1(2), s1(3), s1(4), s1(5), s1(6), s1(7), s1(8), s1(9), s1(10))
//          }
//          val SampleDataOriginal = datardd.toDF("score", "keyid", "rank", "mediaid", "lengthweight", "logplaytimes", "logsearchtimes", "new", "fee", "categoryweight", "srcsearchkey")
//            .withColumn("score2", floor(log(col("score").+(lit(1.7183)))))
//            .drop("score").withColumnRenamed("score2", "score")
//
//          val tdd = sc.textFile("D:\\dataframeData\\englocal02_612")
//
//          val relData = tdd.map { x =>
//            val s1 = x.replace("[", "").replace("]", "").replace(", ", ":").split(",")
//            (s1(0), s1(1))
//          }.toDF("srcsearchkey", "mediaid")
//          //relData.show(1000,false)
//
//
//
//          val data = relData.rdd.take(relData.count.toInt).map { mex =>
//            // println(mex.get(1).toString.replace("WrappedArray(","[\"").replace(")","\"]").replace(", ","\",\""))
//            //      println(mex.get(0).toString)
//            val searchx =
//              """{
//                  "query": {
//                    "bool": {
//                      "filter": [
//                        {
//                          "terms": {
//                            "_id": """ + mex.get(1).toString.replace("WrappedArray(", "[\"").replace(")", "\"]").replace(":", "\",\"") +
//                """
//                          }
//                        },
//                        {
//                          "sltr": {
//                            "_name": "logged_featureset",
//                            "featureset": "2_1_1_v40_vod_english_similarity",
//                            "params": {
//                              "keywords": """ + "\"" + mex.get(0).toString + "\"" +
//                """
//                            }
//                          }
//                        }
//                      ]
//                    }
//                  },
//                  "ext": {
//                    "ltr_log": {
//                      "log_specs": {
//                        "name": "log_entry1",
//                        "named_query": "logged_featureset"
//                      }
//                    }
//                  },
//                 "_source": {
//       	             "includes":  ["log_entry1",
//           "objname"]
//                  }
//                }""".stripMargin
//
//            //println(searchx)
//            //val result = Http("http://10.18.210.224:9600/unionsearch_vod/_search")
//            //val result = Http("http://10.18.210.224:9214/unionsearch_vod_online/_search?size=100")
//            val result = Http("http://10.18.210.224:9214/vod_online/_search?size=100")
//
//              .postData(searchx)
//              .header("Content-Type", "application/json")
//              .header("Charset", "UTF-8")
//              .option(HttpOptions.readTimeout(100000)).asString
//
//
//            val json = JsonPath.parse(result.body)
//            val resultId: JSONArray = json.read("$.hits.hits[*]._id")
//            val resultLog: JSONArray = json.read("$.hits.hits[*].fields._ltrlog[*].log_entry1")
//            val resultName: JSONArray = json.read("$.hits.hits[*]._source")
//
//
//            val idArray = resultId.toArray
//            println(idArray.length)
//            val logArray = resultLog.toArray
//            println(logArray.length)
//            val nameArray = resultName.toArray
//            println("查询关键词:  " + mex.get(0).toString)
//
//
//            val res = logArray.zipWithIndex.map { x =>
//              val index = x._2
//              print(nameArray(x._2))
//              println(x._1)
//            }
//          }
//

      //输出中文结果

      val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
        .set("spark.sql.warehouse.dir", "/spark-warehouse/")
      val sc = new SparkContext(conf)
      val MyContext = new HiveContext(sc)

      val mdd = sc.textFile("D:\\dataframeData\\chnlocal01")

      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      val datardd = mdd.map { x =>
        val s1 = x.replace("[", "").replace("]", "").split(",")
        (s1(0), s1(1), s1(2), s1(3), s1(4), s1(5), s1(6), s1(7), s1(8), s1(9), s1(10))
      }
      val SampleDataOriginal = datardd.toDF("score", "keyid", "rank", "mediaid", "lengthweight", "logplaytimes", "logsearchtimes", "new", "fee", "categoryweight", "srcsearchkey")
        .withColumn("score2", floor(log(col("score").+(lit(1.7183)))))
        .drop("score").withColumnRenamed("score2", "score")

      val tdd = sc.textFile("D:\\dataframeData\\chnlocal02")

      val relData = tdd.map { x =>
        val s1 = x.replace("[", "").replace("]", "").replace(", ", ":").split(",")
        (s1(0), s1(1))
      }.toDF("srcsearchkey", "mediaid")

      val data = relData.rdd.take(relData.count.toInt).map { mex =>
        // println(mex.get(1).toString.replace("WrappedArray(","[\"").replace(")","\"]").replace(", ","\",\""))
        //      println(mex.get(0).toString)
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
                      "featureset": "2_1_1_v40_vod_chinese_similarity",
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
 	             "includes": ["log_entry1",
 	             "objname"]
            }
          }""".stripMargin

        println(searchx)
        //val result = Http("http://10.18.210.224:9600/unionsearch_vod/_search")
        val result = Http("http://10.18.210.224:9214/vod_online/_search?size=100")

          .postData(searchx)
          .header("Content-Type", "application/json")
          .header("Charset", "UTF-8")
          .option(HttpOptions.readTimeout(100000)).asString


      //  println(result)


        val json = JsonPath.parse(result.body)
        val resultId: JSONArray = json.read("$.hits.hits[*]._id")
        val resultLog: JSONArray = json.read("$.hits.hits[*].fields._ltrlog[*].log_entry1")
        val resultName: JSONArray = json.read("$.hits.hits[*]._source")
        println("查询关键词:  " + mex.get(0).toString)

        val idArray = resultId.toArray
        println(idArray.length)
        val logArray = resultLog.toArray
        val nameArray = resultName.toArray


        val res = logArray.zipWithIndex.map { x =>
          val index = x._2

          print(nameArray(x._2))
          println(x._1)

      }
    }

  }
}
