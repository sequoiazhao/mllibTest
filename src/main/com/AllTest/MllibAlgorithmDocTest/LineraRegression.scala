package com.AllTest.MllibAlgorithmDocTest

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.functions._

/**
  * @author zhaoming on 2018-05-29 16:12
  **/
object LineraRegression {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")

    val sc = new SparkContext(conf)

    val data = sc.textFile("d:/data/lpsa.data")

    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()

   // parsedData.foreach(println)

    // Building the model
    val numIterations = 500
    val stepSize = 0.00000001
    val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)

    // Evaluate model on training examples and compute training error
    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()

    valuesAndPreds.foreach(println)

    println("training Mean Squared Error = " + MSE)

    // Save and load model
//    model.save(sc, "myModelPath")
//    val sameModel = LinearRegressionModel.load(sc, "myModelPath")



  }

}
