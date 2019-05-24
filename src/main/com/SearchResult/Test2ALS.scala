package com.SearchResult

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhaoming on 2018-06-27 19:05
  **/
object Test2ALS {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[*]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    //val MyContext = new HiveContext(sc)
    //val mdd = sc.textFile("D:\\dataframeData\\englocal01")

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val ratings = sc.textFile("D:\\data\\sample_movielens_ratings.txt")
      .map(_.replaceAll("::1424380312", "").split("::") match {
        case Array(user, item, rate) =>
          Rating(user.toInt, item.toInt, rate.toDouble)
      })

    // ratings.take(1).foreach(println)

    //      .dropRight(3)

    //    ratings.take(10).foreach(println)
    //
    //   // val data = ratings.drop("timestamp")
    //
    val rank = 2
    val numIterations = 2
    val model = ALS.train(ratings, rank, numIterations, 0.01)
    //ALS.tr

    var rs = model.recommendProducts(2, 10)
    rs.foreach(println)






  }

}
