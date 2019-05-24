package com.Working

import com.MLToolsTest.ChiSqSelectorTest.Bean
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.VertexId
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * @author zhaoming on 2018-07-04 13:11
  **/
object TestVector extends Serializable{


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.FATAL)
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)


    val sqlContext = SQLContext.getOrCreate(sc)

    val hiveContext = new HiveContext(sc)

    val df = hiveContext.createDataFrame(Seq(
      (1.0, 7.0, 7.8, 3.4),
      (0.0, 84, 3.4, 5.6),
      (1.0, 21, 7.8, 4.2),
      (1.0, 7, 7.8, 3.4),
      (0.0, 80, 3.4, 5.6),
      (1.0, 122, 7.8, 9.1),
      (1.0, 217, 7.8, 3.4)
    )).toDF("id", "features", "a1", "a2")



    //    val df = sc.parallelize(
    //      Seq(0.0, 78.9, 3.4, 5.6),
    //      Seq(1.0, 12, 7.8, 9.1),
    //      Seq(1.0, 27, 7.8, 3.4),
    //      Seq(0.0, 78, 3.4, 5.6),
    //      Seq(1.0, 112, 7.8, 9.1),
    //      Seq(1.0, 7, 7.8, 3.4),
    //      Seq(0.0, 84, 3.4, 5.6)
    //    )//.toDF("id", "features", "a1", "a2")

//    val x1 = sc.parallelize(Seq(Array(1.0, 2.0, 3.0, 4.0,8.0)
//      , Array(7.0, 3.0, 3.0, 4.0,1.2)
//      , Array(9.0, 7.0, 3.0, 4.0,2.0))).map(x=>Vectors.dense(x))
//
//    val RM = new RowMatrix(x1)
//    val simic1 = RM.columnSimilarities()
//    //simic1.entries.take(10).foreach(println)
//
//
//
//    val x2= sc.parallelize(Seq(IndexedRow(1,Vectors.dense(1.0, 2.0, 3.0, 4.0,8.0))
//      , IndexedRow(1,Vectors.dense(7.0, 3.0, 3.0, 4.0,1.2))
//      , IndexedRow(1,Vectors.dense(9.0, 7.0, 3.0, 4.0,2.0))))

//
//    val IRM = new IndexedRowMatrix(x2)
//
//    val simic2 = IRM.columnSimilarities()
//
//    simic2.entries.take(10).foreach(println)



//    val s1 = x1.map {
//      x =>
//        val cc = sc.parallelize(x)
//        x1.map {
//          y =>
//            val dd = sc.parallelize(y)
//            val ff = Statistics.corr(cc, dd, "pearson")
//            ff
//        }
//
//    }
//
//   s1.foreach(x=>x.foreach(println))


    //   // df.show()
    //    val data1 = df.map(f=>Vectors.dense(f._1,f._2.toDouble,f._3,f._4))
    //    //data1.foreach(println)
    //
    //    val stat1 = Statistics.corr(data1,"pearson")
    //    df.foreach{x=>
    //      df.foreach{y=>
    //
    //        val dd = sc.parallelize
    //
    //        Statistics.corr(x,y,"pearson")


    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student"))
        , (7L, ("jgonzal", "postdoc"))
        , (5L, ("franklin", "prof"))
        , (2L, ("istoica", "prof"))))




  }



  //}
}
