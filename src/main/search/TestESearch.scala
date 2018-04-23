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



  }


}
