package com.SearchResult

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhaoming on 2018-06-29 16:22
  **/
object TestALScorrelation {

  def parseRating(str: String): Ratings = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Ratings(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[*]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    //val MyContext = new HiveContext(sc)
    //val mdd = sc.textFile("D:\\dataframeData\\englocal01")

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val ratings = sc.textFile("D:\\data\\sample_movielens_ratings.txt")
      .map(parseRating)
      .toDF()

    ratings.show()

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)

    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")
    model.itemFactors.show(20)

    predictions.show(false)

    println(model.rank)


    // MyContext.clearCache()
    //sc.stop()
  }
}
