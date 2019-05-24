package com.Working

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


/**
  * @author zhaoming on 2018-07-19 11:36
  **/
object Test {
  def main(args: Array[String]): Unit = {

    //    var sssx1= "nihao"
    //
    //    sssx1 += "dddd"
    //
    //    println(sssx1)
    //
    //    val sss1 = "nihao"
    //    val ss2 = "hh"
    //
    //    val ttt = "nihao"
    //
    //    val tempt ="[6]"
    //
    //    println(tempt.replaceAll("[|]",""))
    //
    //    ttt match {
    //      case `sss1` => println("nihao")
    //      case `ss2` => println("hh")
    //    }
    //
    //    println(MediaEm.movie)
    //
    //
    //    val find = "剧情片, 爱情片, 英雄,美人,鬼片,院线大片,情景剧"
    //
    //    val reg = "([\u4e00-\u9fa5]+)片|([\u4e00-\u9fa5]+)剧".r
    //
    //    val res = reg.findAllIn(find)
    //
    //    res.foreach(println)
    //
    //    //    val someXml = XML.loadFile("test.xml")
    //    //    val headerField = someXml\"configuration"\"property"
    //    //
    //    //    headerField.foreach(println)
    //
    //    val properties = new Properties()
    //    val in: InputStream = this.getClass.getClassLoader
    //      .getResourceAsStream("config.properties")
    //    properties.load(in)
    //    println(properties.getProperty("Databasename"))
    //
    //    val ins: InputStream = this.getClass.getClassLoader
    //      .getResourceAsStream("test.xml")
    //
    //    val someXml = XML.load(ins)
    //
    //
    //    val headerField = someXml \ "property"
    //
    //    // println(headerField.text.toString.toLowerCase())
    //    var mapTest: Map[String, String] = Map()
    //    //   headerField.foreach(println)
    //    headerField.foreach { x =>
    //      val sss = x \ "name"
    //      val yyy = x \ "value"
    //      mapTest += (sss.text -> yyy.text)
    //    }
    //    val sssv = mapTest.get("filterCategory")
    //    println(sssv.head.r)
    //
    //    val reg2 = sssv.head.r
    //
    //    reg2.findAllIn(find).foreach(println)
    //
    //
    //    val l1 = 5
    //
    //    l1 match {
    //      case 5 =>
    //        println(l1)
    //    }
    //
    //
    //    val sttr = "[2323]"
    //
    //    println(sttr.replaceAll("\\[|\\]", ""))
    //
    //    Array("nihao", "nihao", "wohao").distinct.filter(x => x != "nihao") foreach (println)
    //
    //
    //    //
    //    //    val fieldAttributes = (someXml\"configuration"\"property").map(_\"@name")
    //    //    fieldAttributes.foreach(println)
    //
    //    val sss = Seq("few", "wer", "rw")
    //    val ccc = sss.asInstanceOf[mutable.WrappedArray[String]]
    //
    //
    //    ccc.foreach(println)
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[2]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.executor.memory", "8g")
      .set("spark.driver.memory", "8g")

    val sc = new SparkContext(conf)

    val array = Array(1, 8, 3, 4, 5, 6, 7, 2)

    var si = 1

    val pos = sc.parallelize(array.map { x =>
      si = si + 1
      (si, LabeledPoint(10, Vectors.dense(1.0, 0.0, 3.0)))
    })



    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val pps = pos.toDF("l1", "l2")
      .select(col("l1")
        , col("l2").getItem("label").as("label")
        , col("l2").getItem("features").as("features"))
    pps.show(false)


    val sstee :RDD[Array[Int]] = pps.rdd.groupBy(r=>
    r.getAs[Double]("label")).map(row=>row._2.map(_.getAs[Int]("l1")).toArray)

   // RDD[(Array[Int], Array[Int])]

//    generateNumbers(20, 200).foreach(println)
//    val ssarr = List("id2", "id3")
//
//    pps.filter(col("l1").isin(ssarr: _*)).show()



  }


  def generateNumbers(n: Int, range: Int): List[Int] = {
    val arr = ArrayBuffer.empty[Int]
    val r = scala.util.Random
    while (arr.size < n) {
      val value = r.nextInt(range)
      if (!arr.contains(value)) arr += value
    }
    arr.toList
  }
}
