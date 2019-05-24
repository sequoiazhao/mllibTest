package com.AllTest.GraphX

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

/**
  * @author zhaoming on 2018-02-07 19:12
  **/
object TestBook {
  def main(args: Array[String]): Unit = {
    //增加此条目确保HiveContext可以用，如果没有会有一个tmp报错信息，一直没有解决
    Logger.getLogger("org").setLevel(Level.FATAL)
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
      .set("spark.executor.memory", "5g")
    val sc = new SparkContext(conf)

    val sqlContext = SQLContext.getOrCreate(sc)

    val hiveContext = new HiveContext(sc)

    val graph = GraphLoader.edgeListFile(sc, "D:\\code_test\\mllibtest\\data\\GraphData\\Cit-HepTh.txt")

   graph.inDegrees.take(10).foreach(println)
//    println(graph.inDegrees.reduce((a, b) => if (a._2 > b._2) a else b))
//
//    val ss =Array((12,200),(13,181),(14,200),(15,120),(16,200),(200,200),(200,1))
//
//    println( ss.reduce((a,b)=>if(a._2>b._2)a else b))

    //graph.edges.take(10).foreach(println)
   // graph.vertices.take(10).foreach(println)


    val v = graph.pageRank(0.001).vertices
    //v.take(10).foreach(println)
    println(v.reduce((a,b)=>if(a._2>b._2) a else b))

  }

}
