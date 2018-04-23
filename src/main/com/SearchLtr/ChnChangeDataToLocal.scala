package com.SearchLtr

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

/**
  * @author zhaoming on 2018-04-17 12:34
  **/
object ChnChangeDataToLocal {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val MyContext = new HiveContext(sc)

    val mdd = sc.textFile("D:\\dataframeData\\chinese")

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val datardd = mdd.map { x =>
      val s1 = x.replace("[", "").replace("]", "").split(",")
      (s1(0), s1(1), s1(2), s1(3), s1(4), s1(5), s1(6), s1(7), s1(8), s1(9), s1(10),s1(11))
    }
    val SampleDataOriginal = datardd.toDF("score", "keyid", "rank", "mediaid", "lengthweight", "logplaytimes", "logsearchtimes", "new", "fee", "categoryweight", "srcsearchkey","title")
      .persist(StorageLevel.MEMORY_ONLY_SER)

    //SampleDataOriginal.show(100,false)

    val relDataAll = SampleDataOriginal.filter(
      col("srcsearchkey").===(lit("熊出没"))
        .or(col("srcsearchkey").===(lit("凤囚凰")))
        .or(col("srcsearchkey").===(lit("非诚勿扰")))
        .or(col("srcsearchkey").===(lit("好久不见")))
        .or(col("srcsearchkey").===(lit("奔跑吧")))
        .or(col("srcsearchkey").===(lit("因为遇见你")))
        .or(col("srcsearchkey").===(lit("斗罗大陆")))
        .or(col("srcsearchkey").===(lit("歌手")))
        .or(col("srcsearchkey").===(lit("阳光下的法庭")))
        .or(col("srcsearchkey").===(lit("养母的花样年华")))
        .or(col("srcsearchkey").===(lit("小猪佩奇")))
        .or(col("srcsearchkey").===(lit("老男孩")))
        .or(col("srcsearchkey").===(lit("大宅门")))
        .or(col("srcsearchkey").===(lit("魔都风云")))
        .or(col("srcsearchkey").===(lit("后妈的春天")))
        .or(col("srcsearchkey").===(lit("琅琊榜")))
        .or(col("srcsearchkey").===(lit("汪汪队立大功全集")))
        .or(col("srcsearchkey").===(lit("爱情保卫战")))
        .or(col("srcsearchkey").===(lit("三生三世十里桃花")))
        .or(col("srcsearchkey").===(lit("烈火如歌")))
    )

    //relDataAll.rdd.saveAsTextFile("D:\\dataframeData\\chnlocal01")

    val relData = relDataAll.select("mediaid"
      , "srcsearchkey")
      .distinct()
      .groupBy("srcsearchkey")
      .agg(collect_set("mediaid").as("mediaArray"))
    //relData.rdd.saveAsTextFile("D:\\dataframeData\\chnlocal02")


    relDataAll.select("score","srcsearchkey","mediaid","title").sort("srcsearchkey")show(2000,false)
    relData.show(2000,false)

  }

}
