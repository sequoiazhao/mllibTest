package com.SearchResult

import com.jayway.jsonpath.JsonPath
import net.minidev.json.JSONArray
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scalaj.http.{Http, HttpOptions}

/**
  * @author zhaoming on 2018-04-18 10:26
  **/
object SearchGetinterface {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val MyContext = new HiveContext(sc)

    val mdd = sc.textFile("D:\\dataframeData\\englocal01")

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val datardd = mdd.map { x =>
      val s1 = x.replace("[", "").replace("]", "").split(",")
      (s1(0), s1(1), s1(2), s1(3), s1(4), s1(5), s1(6), s1(7), s1(8), s1(9), s1(10), s1(11))
    }

    val testData = Array("LH", "MH", "XC", "XCA", "SG", "DG", "BP", "DL", "ZH", "XCM", "BX")

    var allString = ""

    testData.map { x =>


      val title = x

      val SampleDataOriginal = datardd.toDF("score", "keyid", "rank", "mediaid", "lengthweight", "logplaytimes", "logsearchtimes", "new", "fee", "categoryweight", "srcsearchkey", "title")
        .filter(col("srcsearchkey").===(lit(title)))
        .select("title", "score", "mediaid", "logplaytimes", "logsearchtimes")
      // .persist(StorageLevel.MEMORY_ONLY_SER)
      val sampleRdd = SampleDataOriginal.rdd.toArray()

      val result = Http("http://api-unisearch.hismarttv.com/searchApi/search/hotword?" +
        "wechatVersionCode=302001117&appVersionName=2017.4.0.0.23.20&deviceId=86100300900000100000060f48982f9a" +
        "&packageName=com.jamdeo.tv.vod&appVersionCode=1000001003&deviceType=1" +
        "&inputType=1&accessToken=&sessionId=7f40b6f9-26ea-4095-b7ba-e9b0fa57a20f" +
        "&license=wasu&sequence=86100300900000100000060f48982f9a_13969118_1511405220525&appType=1" +
        "&customerId=5786421&deviceMsg=LED55NU8800U0000888&version=01.102.019&typeWriting=1" +
        "&index=0&subscriberId=13969118&page=1" +
        "&keyWord=" + title)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(100000)).asString

      //println(result)

      val json = JsonPath.parse(result.body)

      val resultId: JSONArray = json.read("$.searchResultList[*].mediaId")
      val resultName: JSONArray = json.read("$.searchResultList[*].mediaName")

      val idArray = resultId.toArray
      // idArray.foreach(println)
      val resultNameArray = resultName.toArray
      val htmlData = resultNameArray.map(_.toString.replace("<start>", "<font color='red'>").replace("<end>", "</font>"))

      val resultWithModel = Http("http://10.18.210.224:9214/_api/v1.2/_search?" +
        "pinyin=" + title +
        "&from=0&size=100&licence=wasu")
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(100000)).asString

      //println(resultWithModel)

      val jsonWithModel = JsonPath.parse(resultWithModel.body)

      val resultModelId: JSONArray = jsonWithModel.read("$.data[*].objid")
      val resultModelName: JSONArray = jsonWithModel.read("$.data[*].objname")

      val resultModelIdArray = resultModelId.toArray
      val resultModelNameArray = resultModelName.toArray
      val htmlModelData = resultModelNameArray.map(_.toString.replace("<start>", "<font color='red'>").replace("<end>", "</font>"))


      //htmldata.foreach(println)

      val resultValue = idArray.zipWithIndex.map { x =>
        "<tr><td>" + x._2 + "</td><td>" + htmlData(x._2) + "</td><td>" + x._1.toString + "</td><td>||</td><td>" +
          htmlModelData(x._2) ++ "</td><td>" + resultModelIdArray(x._2).toString + "</td><td>||</td><td><font color='green'>" +
          sampleRdd(x._2)(0) + "</font></td><td>" + sampleRdd(x._2)(2) + "</td><td>" + sampleRdd(x._2)(1) + "</td></tr>"
      }

      val sss = "<h1>" + title + "</h1>" + "<table><tr bgcolor='94abff'><td>排序</td><td>线上标题</td><td>线上ID</td><td></td><td>LTR标题</td>" +
        "<td>LTRID</td><td></td><td>日志统计标题</td><td>统计ID</td><td>点击次数</td></tr>" + resultValue.mkString("") + "</table>"
      //println(sss)
      allString = allString + sss
      sss
    }

    val htmlHeadString = "<!DOCTYPE html><html lang='zh-cn'><head><meta charset='utf-8'/></head><body>"
    val endString = "</body>"
    println(htmlHeadString + allString + endString)

  }

}
