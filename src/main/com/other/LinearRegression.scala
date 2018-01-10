package com.other

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.{SparkConf, SparkContext}

object LinearRegression {
  val conf = new SparkConf().setMaster("local").setAppName("CollaborativeFilter")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val data = sc.textFile("D:\\code\\mllibTest\\mldata\\lpsa.txt")

    val parsedData = data.map(line => {
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }).cache()

    val model = LinearRegressionWithSGD.train(parsedData, 100, 0.1) // build model

    println(model.weights)
    val result = model.predict(Vectors.dense(4, 5))

    println(result)

  }

}


//    val ss="1 1"
//    val tt = Vectors.dense(ss.split(" ").map(_.toDouble))
//     println(tt)
//    println(tt.getClass)