package com.MLToolsTest.otherALS

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

object TestALS {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("CollaborativeFilter")


    val sc = new SparkContext(conf)

    val data = sc.textFile("D:\\code_test\\mllibTest\\mldata\\u1.txt")
    data.foreach(println)
    println(data.getClass)

    val ratings = data.map(_.split(' ') match {
      case Array(user,item,rate)=>Rating(user.toInt,item.toInt,rate.toDouble)
    })

    val rank = 2
    val numIterations = 2
    val model = ALS.train(ratings,rank,numIterations,0.01)

    var rs = model.recommendProducts(1,3)
    rs.foreach(println)
  }

}
