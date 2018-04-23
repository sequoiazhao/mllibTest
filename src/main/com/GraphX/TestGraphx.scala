package com.GraphX

/**
  * @author zhaoming on 2018-01-12 11:28
  **/

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext



object TestGraphx {
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
    println(graph.inDegrees.reduce((a, b) => if (a._2 > b._2) a else b))


    // Assume the SparkContext has already been constructed
    // val sc: SparkContext
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
    sc.parallelize(Array((3L, ("rxin", "student"))
      , (7L, ("jgonzal", "postdoc"))
      , (5L, ("franklin", "prof"))
      , (2L, ("istoica", "prof"))))
    // Create an RDD for edges

    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab")
        , Edge(5L, 3L, "advisor")
        , Edge(2L, 5L, "colleague")
        , Edge(5L, 7L, "pi")))

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graphx = Graph(users, relationships, defaultUser)
    graphx.inDegrees.take(2).foreach(println)

    //graphx.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
    // Count all the edges where src > dst
    //graphx.edges.filter(e => e.srcId > e.dstId).count

    //// Count all users which are postdocs
    graphx.vertices.filter{case(id,(name,pos))=>pos=="postdoc"}.foreach(println)

    graphx.edges.filter(e=>e.srcId>e.dstId).foreach(println)

    graphx.edges.filter { case Edge(src, dst, prop) => src > dst }.foreach(println)


  }

}
