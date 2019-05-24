package com.SearchLtr

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhaoming on 2018-04-17 12:34
  **/
object ChnChangeDataToLocalX {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val MyContext = new HiveContext(sc)

    val mdd = sc.textFile("D:\\dataframeData\\unifiedchinese0722all")

    //mdd.take(200).foreach(println)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val datardd = mdd.map { x =>
      val s1 = x.replace("[", "").replace("]", "").split(",")
      //      println(x)
      //      println(s1.length)
      if (s1.length == 13 || s1.length == 12) {
        (s1(0), s1(1), s1(2), s1(3), s1(4), s1(5), s1(6), s1(7), s1(8), s1(9), s1(10), s1(11))
      } else {
        ("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")
      }
    }
    val SampleDataOriginal = datardd.toDF("score", "keyid", "rank", "mediaid", "lengthweight", "logplaytimes", "logsearchtimes", "new", "fee", "categoryweight", "srcsearchkey", "title")
      .persist(StorageLevel.MEMORY_ONLY_SER)

    val parsedRDD = sc.textFile("D:\\data\\paramsdata.txt")
      .map(_.replace(",","#")split("\\|")).map(x => (x(0), x(1)))
    //parsedRDD.foreach(println)

    val dfSearch = sqlContext.createDataFrame(parsedRDD).toDF(
      "srcsearchkey", "searchlabel"
    ).cache()
    dfSearch.show()


    val relDataAll = SampleDataOriginal.filter(
      col("srcsearchkey").===(lit("扶摇"))
        .or(col("srcsearchkey").===(lit("一千零一夜")))
        .or(col("srcsearchkey").===(lit("斗罗大陆")))
        .or(col("srcsearchkey").===(lit("蜡笔小新")))
        .or(col("srcsearchkey").===(lit("爱情公寓2")))
        .or(col("srcsearchkey").===(lit("中餐厅第2季")))
        .or(col("srcsearchkey").===(lit("阿木木历")))
        .or(col("srcsearchkey").===(lit("微微一笑很倾城")))
        .or(col("srcsearchkey").===(lit("奥特曼")))
        .or(col("srcsearchkey").===(lit("岁岁年年柿柿红")))
        .or(col("srcsearchkey").===(lit("小本")))
        .or(col("srcsearchkey").===(lit("屌德斯")))
        .or(col("srcsearchkey").===(lit("天乩之白蛇传说")))
        .or(col("srcsearchkey").===(lit("明日之子第2季")))
        .or(col("srcsearchkey").===(lit("爱情公寓3")))
        .or(col("srcsearchkey").===(lit("萌妃驾到")))
        .or(col("srcsearchkey").===(lit("答案")))
        .or(col("srcsearchkey").===(lit("熊出没")))
        .or(col("srcsearchkey").===(lit("延禧攻略")))
        .or(col("srcsearchkey").===(lit("火线精英宝哥")))
    )

    relDataAll.show()
    relDataAll.filter(col("srcsearchkey").===(lit("奥特曼"))).show(false)
    //relDataAll.rdd.saveAsTextFile("D:\\dataframeData\\chnlocal01_722")

    val relData = relDataAll.select("mediaid"
      , "srcsearchkey")
      .distinct()
      .groupBy("srcsearchkey")
      .agg(collect_set("mediaid").as("mediaArray"))
      .join(dfSearch, Seq("srcsearchkey"))
    //relData.rdd.saveAsTextFile("D:\\dataframeData\\chnlocal02_722")


    //relData.select("score", "srcsearchkey", "mediaid", "title", "searchlabel").sort("srcsearchkey") show(2000, false)
    relData.show(2000, false)
    relData.filter(col("srcsearchkey").===(lit("奥特曼"))).show(false)

  }

}
