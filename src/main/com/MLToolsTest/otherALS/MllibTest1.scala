package com.MLToolsTest.otherALS

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}


object MllibTest1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("testSummary")

    val sc = new SparkContext(conf)


    //    val rdd = sc.textFile("d://a.txt")
    //      .map(_.split(' ').map(_.toDouble))
    //      .map(line => Vectors.dense(line))
    //
    //
    //    val summary = Statistics.colStats(rdd)
    //    //rdd.foreach(println)
    //    //println(summary.mean)
    //    //println(summary.variance)
    //    println(summary.normL1)
    //    println(summary.normL2)

    val rddX = sc.textFile("d://x.txt")
      .flatMap(_.split(' ')
        .map(_.toDouble))
      .map(line=>Vectors.dense(line))

    println(Statistics.corr(rddX,"spearman"))

//    val rddY = sc.textFile("d://y.txt")
//      .flatMap(_.split(' ')
//          .map(_.toDouble))

    //val correlation:Double = Statistics.corr(rddX,rddY)
//    rddX.foreach(println)


  }

}
