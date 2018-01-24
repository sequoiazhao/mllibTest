package com.other

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}

object LinearRegression3 {
  val conf = new SparkConf().setMaster("local").setAppName("CollaborativeFilter")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val data = sc.textFile("D:\\code_test\\mllibTest\\mldata\\lpsa.txt")

    val parsedData = data.map(line => {
      val parts = line.split(',')
      //生产特征向量
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
      //归一化
    }).map(f=>LabeledPoint(f.label/800,Vectors.dense(f.features(0)/3000,f.features(1)/4))).cache()

  println(parsedData.take(1).foreach(println))

    val model = LinearRegressionWithSGD.train(parsedData, 100, 0.1) // build model

    val valuesAndPreds = parsedData.map{point=>{
      val prediction = model.predict(point.features)
      (point.label,prediction)
    }}

    valuesAndPreds.foreach(println)

    val MSE = valuesAndPreds.map{case(v,p)=>math.pow((v-p),2)}.mean()
    println(MSE)

  }

}
