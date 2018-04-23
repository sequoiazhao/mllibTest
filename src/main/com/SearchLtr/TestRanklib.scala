package com.SearchLtr

import com.jayway.jsonpath.JsonPath
import net.minidev.json.JSONArray
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods.parse

import scalaj.http.{Http, HttpOptions}


/**
  * @author zhaoming on 2018-04-11 19:24
  **/
object TestRanklib {
  val numIterations = 100

  val maxDepth = 2

  val featureStart = 4

  val featureEnd = 10


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val MyContext = new HiveContext(sc)

    //val mdd = sc.textFile("D:\\dataframeData\\englocal01")
    val mdd = sc.textFile("D:\\dataframeData\\engalldata1")

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val datardd = mdd.map { x =>
      val s1 = x.replace("[", "").replace("]", "").split(",")
      (s1(0), s1(1), s1(2), s1(3), s1(4), s1(5), s1(6), s1(7), s1(8), s1(9), s1(10))
    }
    val relDataAll = datardd.toDF("score", "keyid", "rank", "mediaid", "lengthweight", "logplaytimes", "logsearchtimes", "new", "fee", "categoryweight", "srcsearchkey")
      .withColumn("score2", floor(log(col("score").+(lit(1.7183)))))
      .drop("score").withColumnRenamed("score2", "score")

    val tdd = sc.textFile("D:\\dataframeData\\engalldata")

    // tdd.foreach(println)

    val dataTestrdd = tdd.map { x =>
      val s1 = x.replace("[", "").replace("]", "").replace(", ", ":").split(",")
      (s1(0), s1(1))
    }.toDF("srcsearchkey", "mediaid")


    val data = dataTestrdd.rdd.map { mex =>
      //println(mex.get(1).toString.replace("WrappedArray(", "[\"").replace(")", "\"]").replace(":", "\",\""))

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
                      "featureset": "v40_vod_english_similarity",
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
 	             "includes": "log_entry1"
            }
          }""".stripMargin

     // println(searchx)
      val result = Http("http://10.18.210.224:9214/unionsearch_vod_online/_search?size=100")
        .postData(searchx)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(100000)).asString

     // println(result)

      val json = JsonPath.parse(result.body)
      val resultId: JSONArray = json.read("$.hits.hits[*]._id")
      val resultLog: JSONArray = json.read("$.hits.hits[*].fields._ltrlog[*].log_entry1")


      val idArray = resultId.toArray
      println(idArray.length)
      val logArray = resultLog.toArray

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

      idArray.zipWithIndex.map { x =>
        (mex.get(0).toString, x._1.toString, LogValueArray(x._2).toString.toDouble * 10)
      }
    }.reduce((x, y) => x.union(y))

    val sdata = MyContext.createDataFrame(data.toSeq)
      .toDF("srcsearchkey", "mediaid", "tfidf")

    sdata.show()
    val joinData = relDataAll.join(sdata, Seq("srcsearchkey", "mediaid"))
      .select("score", "keyid", "rank", "mediaid", "lengthweight", "logplaytimes", "logsearchtimes", "new", "fee", "categoryweight", "tfidf", "srcsearchkey")
    //.filter(col("srcsearchkey").===(lit("CFS")))
    // joinData.show()

    val testRdd = ChangeRanklibSampleData(joinData, featureStart, featureEnd)

    //testRdd.foreach(println)
    testRdd.repartition(1).saveAsTextFile("D:\\model_test\\sampleall.txt")

  }


  def ChangeRanklibSampleData(data: DataFrame, featureBegin: Int, featureEnd: Int): RDD[String] = {

    val dataRDD = data.rdd.map { ss =>
      val da1 = ss.get(0)
      val da2 = "qid:" + ss.get(1)
      val da3 = ss.get(2)
      var da4 = ""
      for (i <- featureBegin to featureEnd) {
        da4 = da4 + " " + (i - 3).toString + ":" + ss.get(i)
      }
      da1.toString + " " + da2.toString + " " + da4

    }
    dataRDD
  }


}
