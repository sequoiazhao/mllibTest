package com.Working

import com.Common.GetConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SQLContext

/**
  * @author zhaoming on 2018-09-13 16:21
  **/
object TestALS {

  def main(args: Array[String]): Unit = {
    val getConf = new GetConf()
    val sc = getConf.getConfSingle
    sc.setCheckpointDir("checkpoint")

    val sqlContext = new SQLContext(sc)

    val data = sc.textFile("d:\\data\\test.data")
    val ratings = data.map(_.split(',') match {
      case Array(user, item, rate) => Rating(user.toInt, item.toInt, rate.toDouble)
    })
    ratings.foreach(println)


    val rank = 10 //潜在因素

    val numIterations = 10

    val model = ALS.train(ratings, rank, numIterations, 0.05)

    //    val result = model.recommendProductsForUsers(3)
    //    result.foreach(x=>println(x._1+"----"+x._2.mkString("#")))

    //model.recommendProducts(2, 2).foreach(println)

    val userProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }

    val predictions = model.predict(userProducts).map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }
    predictions.foreach(println)

    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }

    ratesAndPreds.foreach(println)
    //   userProducts.foreach(println)

    val testData = ratesAndPreds.join(predictions)

    testData.foreach(println)

    val MSE = testData.map { case ((user, product), (r1, r2)) =>
      val res = (r1 - r2) * (r1 - r2)
      println(res)
      res
    }.mean()


    println("Mean squared error = " + MSE)

    model.save(sc,"d:\\data\\model\\001")


  }


}
