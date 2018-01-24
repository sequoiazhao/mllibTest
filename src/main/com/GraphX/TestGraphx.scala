package com.GraphX

/**
  * @author zhaoming on 2018-01-12 11:28
  **/

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext



object TestGraphx {
  def main(args: Array[String]): Unit = {
    //增加此条目确保HiveContext可以用，如果没有会有一个tmp报错信息，一直没有解决
    Logger.getLogger("org").setLevel(Level.FATAL)
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)

    val sqlContext = SQLContext.getOrCreate(sc)

    val hiveContext = new HiveContext(sc)

    val graph = GraphLoader.edgeListFile(sc,"D:\\code_test\\mllibtest\\data\\GraphData\\Cit-HepTh.txt")

    graph.inDegrees.take(10).foreach(println)
    println(graph.inDegrees.reduce((a,b)=>if(a._2>b._2) a else b))


  }

}
