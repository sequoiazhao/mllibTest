package com.SearchLtr

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

/**
  * @author zhaoming on 2018-04-12 11:15
  **/
object EngChangeDataToLocal {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val MyContext = new HiveContext(sc)

    val mdd = sc.textFile("D:\\dataframeData\\unifiedenglish0720")

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    //mdd.take(300).foreach(println)

    val datardd = mdd.map { x =>
      val s1 = x.replace("[", "").replace("]", "").split(",")

      println(s1.length)
      if (s1.length == 12) {
        (s1(0), s1(1), s1(2), s1(3), s1(4), s1(5), s1(6), s1(7), s1(8), s1(9), s1(10), s1(11))
      } else {
        ("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")
      }
    }
    val SampleDataOriginal = datardd.toDF("score", "keyid", "rank", "mediaid", "lengthweight", "logplaytimes", "logsearchtimes", "new", "fee", "categoryweight", "srcsearchkey", "title")
      .persist(StorageLevel.MEMORY_ONLY_SER)
    //SampleDataOriginal.show()

    //    val relData1 = SampleDataOriginal.limit(800)
    //    val relData2 = SampleDataOriginal.filter(col("srcsearchkey").===(lit("BX")).or(col("srcsearchkey").===(lit("XC"))))
    //    val relDataAll = relData1.unionAll(relData2)

    // println(SampleDataOriginal.count())

    //LH, MH , XC , XCA, SG , DG , BP , DL , ZH , XCM
    val relDataAll = SampleDataOriginal.filter(
      col("srcsearchkey").===(lit("GX"))
        .or(col("srcsearchkey").===(lit("DMB")))
        .or(col("srcsearchkey").===(lit("FN")))
        .or(col("srcsearchkey").===(lit("GLF")))
        .or(col("srcsearchkey").===(lit("HY")))
        .or(col("srcsearchkey").===(lit("ZH")))
        .or(col("srcsearchkey").===(lit("HS")))
        .or(col("srcsearchkey").===(lit("DL")))
        .or(col("srcsearchkey").===(lit("JA")))
        .or(col("srcsearchkey").===(lit("SDQ")))

        .or(col("srcsearchkey").===(lit("TQ")))
        .or(col("srcsearchkey").===(lit("SH")))
        .or(col("srcsearchkey").===(lit("WN")))
        .or(col("srcsearchkey").===(lit("XX")))
        .or(col("srcsearchkey").===(lit("XCM")))
        .or(col("srcsearchkey").===(lit("HL")))
        .or(col("srcsearchkey").===(lit("CZ")))
        .or(col("srcsearchkey").===(lit("SS")))
        .or(col("srcsearchkey").===(lit("WS")))
        .or(col("srcsearchkey").===(lit("BP")))
    )
    //relDataAll.show()
    relDataAll.rdd.saveAsTextFile("D:\\dataframeData\\englocal01_720")

    //SampleDataOriginal.rdd.saveAsTextFile("D:\\dataframeData\\engalldata1")
    // val relData = SampleDataOriginal.select("mediaid"
    val relData = relDataAll.select("mediaid"
      , "srcsearchkey")
      .distinct()
      .groupBy("srcsearchkey")
      .agg(collect_set("mediaid").as("mediaArray"))

    //relData.rdd.saveAsTextFile("D:\\dataframeData\\engalldata")
    relData.rdd.saveAsTextFile("D:\\dataframeData\\englocal02_720")
    //
    //
    relDataAll.select("score", "srcsearchkey", "mediaid", "title").show(2000, false)
    //    relData.show(2000,false)
  }

}
