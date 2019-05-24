package com.SearchResult

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

/**
  * @author zhaoming on 2018-06-14 11:25
  **/
object TestDataFrame {
  def main(args: Array[String]): Unit = {
    //增加此条目确保HiveContext可以用，如果没有会有一个tmp报错信息，一直没有解决
    Logger.getLogger("org").setLevel(Level.FATAL)
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)


    val sqlContext = SQLContext.getOrCreate(sc)

    val hiveContext = new HiveContext(sc)


    val df = hiveContext.createDataFrame(Seq(
      //      (0.0, Vectors.dense(1.0, 0.5, -1.0)),
      //      (1.0, Vectors.dense(2.0, 1.0, 1.0)),
      //      (1.0, Vectors.dense(3.0, 5.0, 4.0)),
      //      (2.0, Vectors.dense(4.0, 10.0, 2.0))
      //      (0.0, Array(1.0, 0.5, -1.0), 3.4, 5.6),
      //      (1.0, Array(2.0, 1.0, 1.0), 7.8, 9.1),
      //      (1.0, Array(4.0, 10.0, 2.0), 7.8, 3.4)

      (0.0, 78, 3.4, 5.6),
      (1.0, 12, 7.8, 9.1),
      (1.0, 27, 7.8, 3.4),
      (0.0, 78, 3.4, 5.6),
      (1.0, 112, 7.8, 9.1),
      (1.0, 7, 7.8, 3.4),
      (0.0, 84, 3.4, 5.6),
      (1.0, 21, 7.8, 4.2),
      (1.0, 7, 7.8, 3.4),
      (0.0, 80, 3.4, 5.6),
      (1.0, 122, 7.8, 9.1),
      (1.0, 217, 7.8, 3.4)
    )).toDF("id", "features", "a1", "a2")



    df.show()
    df.describe().show()
    //    println(df.stat.cov("a1","a2"))
    //
    //    println(df.stat.corr("a1","a2"))

    df.stat.crosstab("a1", "a2").orderBy("a1_a2").show(10)

    val freq = df.stat.freqItems(Seq("a1"), 0.3)
    freq.show()

    df.registerTempTable("call")

    val df3 = df.groupBy("a1").pivot("id").sum("a2").na.fill(0)

    df3.show()


    val dfrdd = df.rdd
    val header = dfrdd.first()
    val dff = dfrdd.filter(row => row != header)
    //.filter(row=> !row.contains("?"))
    dff.foreach(println)

//    val num = df.groupBy("a2").agg(countDistinct("id")).count()
//
//    println("ssss"+num)

    //case class HD(ss1:Double,ss2:Integer,ss3:Double,ss4:Double)
//
//    import sqlContext.implicits._
//    val xtd = df.as[HD]
//
//    xtd.show()

    df.printSchema()

    df.show()

    val f = Seq(0.0)

   // df.select(col("id").isin(f:_*)).show()

    df.filter(col("id").isin(f:_*)).show()



  }

}
