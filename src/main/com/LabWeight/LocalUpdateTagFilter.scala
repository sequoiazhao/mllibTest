package com.LabWeight

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

import scalaj.http.{Http, HttpOptions}

/**
  * @author zhaoming on 2018-05-16 14:25
  **/
object LocalUpdateTagFilter {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val MyContext = new HiveContext(sc)

    val mdd = sc.textFile("D:\\dataframeData\\01filter5").filter(_.nonEmpty)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    mdd.take(20).foreach(println)
    val dataRdd = mdd.map { x =>
      val s1 = x.replace("[", "").replace("]", "").split(",")
      if (s1.length == 3) {
        (s1(0), s1(1), "\""+s1(2).replace("#", "\",\"")+"\"")
      }
      else {
        (null, null, null)
      }
    }
    val SampleData1 = dataRdd.toDF("id", "title", "tagfilter")
    SampleData1.show(false)
    val SampleData = SampleData1.filter(col("id").isNotNull)

    SampleData.foreach { line =>

      val mediaId = line.get(0)
      val stringStr = line.get(2)
      val update =
        s"""
        {
        	"doc":{
        			"taga":[$stringStr]
        	}

        }
      """.stripMargin

      val httpStr = "http://10.18.210.224:9210/vod_online/vod/" + mediaId + "/_update"

      val result = Http(httpStr)
        .postData(update)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(100000)).asString

      println(result)

    }

  }

}
