package com.SearchLtr

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

/**
  * @author zhaoming on 2018-04-12 11:15
  **/
object ChangeDataToLocal {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val MyContext = new HiveContext(sc)

    val mdd = sc.textFile("D:\\dataframeData\\english")

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val datardd = mdd.map { x =>
      val s1 = x.replace("[", "").replace("]", "").split(",")
      (s1(0), s1(1), s1(2), s1(3), s1(4), s1(5), s1(6), s1(7), s1(8), s1(9), s1(10),s1(11))
    }
    val SampleDataOriginal = datardd.toDF("score", "keyid", "rank", "mediaid", "lengthweight", "logplaytimes", "logsearchtimes", "new", "fee", "categoryweight", "srcsearchkey","title")
      .persist(StorageLevel.MEMORY_ONLY_SER)
    //SampleDataOriginal.show()

//    val relData1 = SampleDataOriginal.limit(800)
//    val relData2 = SampleDataOriginal.filter(col("srcsearchkey").===(lit("BX")).or(col("srcsearchkey").===(lit("XC"))))
//    val relDataAll = relData1.unionAll(relData2)

   // println(SampleDataOriginal.count())

    //LH, MH , XC , XCA, SG , DG , BP , DL , ZH , XCM
//    val relDataAll = SampleDataOriginal.filter(
//      col("srcsearchkey").===(lit("LH"))
//        .or(col("srcsearchkey").===(lit("MH")))
//        .or(col("srcsearchkey").===(lit("XC")))
//        .or(col("srcsearchkey").===(lit("XCA")))
//        .or(col("srcsearchkey").===(lit("SG")))
//        .or(col("srcsearchkey").===(lit("DG")))
//        .or(col("srcsearchkey").===(lit("BP")))
//        .or(col("srcsearchkey").===(lit("DL")))
//        .or(col("srcsearchkey").===(lit("ZH")))
//        .or(col("srcsearchkey").===(lit("XCM")))
//        .or(col("srcsearchkey").===(lit("BX")))
//        .or(col("srcsearchkey").===(lit("HM")))
//        .or(col("srcsearchkey").===(lit("AQ")))
//        .or(col("srcsearchkey").===(lit("CC")))
//        .or(col("srcsearchkey").===(lit("HY")))
//        .or(col("srcsearchkey").===(lit("HL")))
//        .or(col("srcsearchkey").===(lit("NN")))
//        .or(col("srcsearchkey").===(lit("KL")))
//        .or(col("srcsearchkey").===(lit("SD")))
//        .or(col("srcsearchkey").===(lit("ZJS")))
//    )
//
//    relDataAll.rdd.saveAsTextFile("D:\\dataframeData\\englocal01")
//

    SampleDataOriginal.rdd.saveAsTextFile("D:\\dataframeData\\engalldata1")
//    val relData = SampleDataOriginal.select("mediaid"
//      , "srcsearchkey")
//      .distinct()
//      .groupBy("srcsearchkey")
//      .agg(collect_set("mediaid").as("mediaArray"))
//
//    relData.rdd.saveAsTextFile("D:\\dataframeData\\engalldata")
//    relData.rdd.saveAsTextFile("D:\\dataframeData\\englocal02")
//
//
//    relDataAll.select("score","srcsearchkey","mediaid","title").show(2000,false)
//    relData.show(2000,false)
  }

}
