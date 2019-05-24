package search

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
//import org.elasticsearch.spark._

/**
  * @author zhaoming on 2018-02-28 11:16
  **/
object TestESearch {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "127.0.0.1")
      .set("es.port", "9200")

    val sc = new SparkContext(conf)
//    val sqlContext = SQLContext.getOrCreate(sc)
//
    val resource = "secisland/secilog"

    //val accountRDD = sc.esJsonRDD(resource)

    //accountRDD.foreach(t=>println(t._1+"==="+t._2))

    val ssst = Seq(0, 1, 2, 3, 4, 4, 2, 1, 0, 1, 1, 2, 5)

    val sssc = sc.parallelize(ssst)

    val count = sssc.map(x=>(x,1)).reduceByKey(_+_).collect()
    count.foreach(println)

    val count2 =ssst.map(x=>(x,1)).toList
      .groupBy(_._1).map { x =>
      val key = x._1
      val ttv = x._2.map(_._2).mkString(",")
      (key, ttv.split(",").map(x => x.toDouble).sum)

    }.toList.sortBy(_._2).reverse
    count2.foreach(println)




  }


}
