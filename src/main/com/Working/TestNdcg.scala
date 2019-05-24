package com.Working

import com.Common.GetConf
import org.apache.spark.mllib.evaluation.{RankingMetrics, RegressionMetrics}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SQLContext

/**
  * @author zhaoming on 2018-08-22 17:55
  **/
object TestNdcg {

  def main(args: Array[String]): Unit = {
    val getConf = new GetConf()
    val sc = getConf.getConfSingle

    val sqlContext = new SQLContext(sc)

    val ratings = sc.textFile("d:\\data\\sample_movielens_data.txt").map { line =>
      val fields = line.split("::")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble - 2.5)
    }.cache()

    //ratings.foreach(println)

    //样本处理，二元化
    val binarizedRatings = ratings.map(r => Rating(r.user, r.product,
      if (r.rating > 0) 1.0 else 0.0)).cache()


    val numRatings = ratings.count()
    val numUsers = ratings.map(_.user).distinct().count()
    val numMovies = ratings.map(_.product).distinct().count()
    println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")
    //1501条记录，30个用户，100个电影


    // Build the model
    val numIterations = 15
    val rank = 50 //
    val lambda = 0.01
    val alpha = 0.01
    val model = ALS.train(ratings, rank, numIterations, lambda)
    //val model = ALS.trainImplicit(ratings, rank, numIterations, lambda, alpha)

    // Define a function to scale ratings from 0 to 1
    def scaledRating(r: Rating): Rating = {
      val scaledRating = math.max(math.min(r.rating, 1.0), 0.0)
      Rating(r.user, r.product, scaledRating)
    }

    //    val userResult = model.recommendProductsForUsers(4)
    //    userResult.foreach{x=>
    //      println("用户:"+x._1)
    //      x._2.foreach(println)
    //    }

    // Get sorted top ten predictions for each user and then scale from [0, 1]
    //结果大于1的都是相关的，小于1的值输出
    val userRecommended = model.recommendProductsForUsers(10).map { case (user, recs) =>
      (user, recs.map(scaledRating))
    }

    //    userRecommended.foreach{x=>
    //      println(x._1)
    //      x._2.foreach(println)
    //    }

    // Assume that any movie a user rated 3 or higher (which maps to a 1) is a relevant document
    // Compare with top ten most relevant documents


    val userMovies = binarizedRatings.groupBy(_.user)


    userMovies.foreach { x =>
      println(x._1)
      x._2.foreach(println)
    }

    userRecommended.foreach { x =>
      println(x._1)
      x._2.foreach(println)
    }


    val temp = userMovies.join(userRecommended)


    val rel = temp.map {
      case (user, (actual, predictions)) =>
        (predictions.map(_.product), actual.filter(_.rating > 0.0).map(_.product).toArray, user)

    }
    rel.foreach(x => println(x._1.mkString(",") + "-------" + x._2.mkString(",") + "==" + x._3))


    val relevantDocuments = userMovies.join(userRecommended).map { case (user, (actual,
    predictions)) =>
      (predictions.map(_.product), actual.filter(_.rating > 0.0).map(_.product).toArray)
    }


    val array = Array(1, 2, 3, 4, 5, 6, 7)
    val testData = sc.parallelize(array.map(x => (Array(2, 3, 4, 5, 6, 7, 8, 9), Array(7, 8, 9, 5, 4, 3, 2))))

    testData.take(7).foreach(println)

    // Instantiate metrics object
    val metrics = new RankingMetrics(testData)



    // Precision at K
    Array(1, 2, 3, 4, 5, 6).foreach { k =>
      println(s"Precision at $k = ${metrics.precisionAt(k)}")
    }

    // Mean average precision
    println(s"Mean average precision = ${metrics.meanAveragePrecision}")

    // Normalized discounted cumulative gain
    Array(1, 2, 3, 4, 5).foreach { k =>
      println(s"NDCG at $k = ${metrics.ndcgAt(k)}")
    }

    // Get predictions for each data point
    val allPredictions = model.predict(ratings.map(r => (r.user, r.product))).map(r => ((r.user,
      r.product), r.rating))
    val allRatings = ratings.map(r => ((r.user, r.product), r.rating))
    val predictionsAndLabels = allPredictions.join(allRatings).map { case ((user, product),
    (predicted, actual)) =>
      (predicted, actual)
    }

    // Get the RMSE using regression metrics
    val regressionMetrics = new RegressionMetrics(predictionsAndLabels)
    println(s"RMSE = ${regressionMetrics.rootMeanSquaredError}")

    // R-squared
    println(s"R-squared = ${regressionMetrics.r2}")
    // $example off$
  }


}
