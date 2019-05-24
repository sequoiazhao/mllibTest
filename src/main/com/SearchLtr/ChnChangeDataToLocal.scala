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

    val mdd = sc.textFile("D:\\dataframeData\\chinese0702")

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val datardd = mdd.map { x =>
      val s1 = x.replace("[", "").replace("]", "").split(",")
//      println(x)
//      println(s1.length)
      if (s1.length == 13) {
        (s1(0), s1(1), s1(2), s1(3), s1(4), s1(5), s1(6), s1(7), s1(8), s1(9), s1(10), s1(11))
      } else {
        ("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")
      }
    }
    val SampleDataOriginal = datardd.toDF("score", "keyid", "rank", "mediaid", "lengthweight", "logplaytimes", "logsearchtimes", "new", "fee", "categoryweight", "srcsearchkey","title")
      .persist(StorageLevel.MEMORY_ONLY_SER)

   // SampleDataOriginal.show(1000,false)

    val relDataAll = SampleDataOriginal.filter(
      col("srcsearchkey").===(lit("熊出没"))
        .or(col("srcsearchkey").===(lit("第五人格")))
        .or(col("srcsearchkey").===(lit("非诚勿扰")))
        .or(col("srcsearchkey").===(lit("火影忍者")))
        .or(col("srcsearchkey").===(lit("奔跑吧")))
        .or(col("srcsearchkey").===(lit("爱情公寓")))
        .or(col("srcsearchkey").===(lit("斗罗大陆")))
        .or(col("srcsearchkey").===(lit("西游记")))
        .or(col("srcsearchkey").===(lit("哆啦A梦")))
        .or(col("srcsearchkey").===(lit("微微一笑很倾城")))
        .or(col("srcsearchkey").===(lit("小猪佩奇")))
        .or(col("srcsearchkey").===(lit("泡沫之夏")))
        .or(col("srcsearchkey").===(lit("宫心计")))
        .or(col("srcsearchkey").===(lit("魔都风云")))
        .or(col("srcsearchkey").===(lit("后妈的春天")))
        .or(col("srcsearchkey").===(lit("我的世界")))
        .or(col("srcsearchkey").===(lit("蜡笔小新")))
        .or(col("srcsearchkey").===(lit("奥特曼大战")))
        .or(col("srcsearchkey").===(lit("三生三世十里桃花")))
        .or(col("srcsearchkey").===(lit("烈火如歌")))
    )

   // relDataAll.show()
    relDataAll.rdd.saveAsTextFile("D:\\dataframeData\\chnlocal01_702")

    val relData = relDataAll.select("mediaid"
      , "srcsearchkey")
      .distinct()
      .groupBy("srcsearchkey")
      .agg(collect_set("mediaid").as("mediaArray"))
    relData.rdd.saveAsTextFile("D:\\dataframeData\\chnlocal02_702")


    relDataAll.select("score","srcsearchkey","mediaid","title").sort("srcsearchkey")show(2000,false)
    relData.show(2000,false)

  }

}
