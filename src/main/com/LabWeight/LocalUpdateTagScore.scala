package com.LabWeight

import com.Common.GetConf
import com.jayway.jsonpath.JsonPath
import net.minidev.json.JSONArray
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

import scalaj.http.{Http, HttpOptions}


/**
  * @author zhaoming on 2018-05-13 22:23
  **/
object LocalUpdateTagScore {

  def main(args: Array[String]): Unit = {

    val getConf = new GetConf()
    val (sc, myContext) = getConf.getConf

    val mdd = sc.textFile("D:\\dataframeData\\weightNested912_2").filter(_.nonEmpty)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._


    val dataRdd = mdd.map { x =>
      //val s1 = x.replace("[", "").replace("]", "").replace("\",\"", "\"#\"").replace("},{", "}#{").split(",")
//      println(x)
      val s1 = x.replace("[", "").replace("]", "").replace("""WrappedArray(""","")
        .replace(""")""","").replace("\",\"", "\"#\"").replace(",\"", "#\"").replace("}, {", "}#{").split(",")
     s1.foreach(println)
      if (s1.length == 3)
        {(s1(0), s1(1), s1(2).replace("#", ","))}
      else
        {(null,null,null)}
    }
    val SampleData1 = dataRdd.toDF("id", "title", "tagscore")

    val SampleData = SampleData1.filter(col("id").isNotNull)

    println(SampleData.count())
    SampleData.show(false)

//    SampleData.foreach { line =>
//
//      val mediaId = line.get(0)
//      val stringStr = line.get(2)
//
//      //println(stringStr)
//      val update =
//              s"""
//              {
//              	"doc":{
//              			"tag_score":[$stringStr]
//              	}
//
//              }
//            """.stripMargin
//
//            val httpStr = "http://10.18.216.249:9210/unionsearch_vod/vod/" + mediaId + "/_update"
//
//            val result = Http(httpStr)
//              .postData(update)
//              .header("Content-Type", "application/json")
//              .header("Charset", "UTF-8")
//              .option(HttpOptions.readTimeout(100000)).asString
//
//            println(result)
//
//
//    }

  }

}

//SampleData.filter(col("id").===(lit("11012380431"))).show(false)


//    SampleData.rdd.take(SampleData.count.toInt).map{line=>
//      val mediaId = line.get(0)
//
//
//    }
// SampleData.filter(col("id").===(lit("11012380431"))).foreach{line=>


//    val mediaId = "11012380431"
////    val name ="你好吗"
//    //    val value = 3
//    //    val updatebyScript =
//    //      s"""
//    //        {
//    //        	"script":"for(i in ctx._source.tag_score){i.name='$name';i.value=$value}"
//    //
//    //        }
//    //      """.stripMargin
//
//    val stringStr ="""{"name":"戏剧","value":34.94},{"name":"养眼","value":9.7},{"name":"爱情片","value":1.97}"""
//
//
//    val update =
//      s"""
//        {
//        	"doc":{
//        			"tag_score":[$stringStr]
//        	}
//
//        }
//      """.stripMargin
//
//    val httpStr = "http://10.18.210.224:9210/vod_online/vod/"+mediaId+"/_update"
//
//
//    val result = Http(httpStr)
//      .postData(update)
//      .header("Content-Type", "application/json")
//      .header("Charset", "UTF-8")
//      .option(HttpOptions.readTimeout(100000)).asString
//
//    println(result)

//"id", colTitle, "tagscore"