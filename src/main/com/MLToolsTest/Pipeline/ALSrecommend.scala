package com.MLToolsTest.Pipeline

/**
  * @author zhaoming on 2018-06-20 17:38
  **/


// $example on$
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
// $example off$


/**
  * An example demonstrating ALS.
  * Run with
  * {{{
  * bin/run-example ml.ALSExample
  * }}}
  */


object ALSrecommend {

  // $example on$
  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  // $example off$

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)


    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    // $example on$
    val ratings = sc.textFile("D:\\data\\sample_movielens_ratings.txt")
      .map(parseRating)
      .toDF()
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    training.show()

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    //model.setColdStartStrategy("drop")
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    // Generate top 10 movie recommendations for each user
    //val userRecs = model.recommendForAllUsers(10)
    // Generate top 10 user recommendations for each movie
    //val movieRecs = model.recommendForAllItems(10)
    // $example off$
    //userRecs.show()
    //movieRecs.show()
    model.itemFactors.show(1000, false)
    model.userFactors

    sc.stop()
  }

}
