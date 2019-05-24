package com.AllTest.MllibAlgorithmDocTest

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.functions._

/**
  * @author zhaoming on 2018-05-29 16:01
  **/
object linearSVM {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "127.0.0.1")
      .set("es.port", "9200")

    val sc = new SparkContext(conf)

    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "d:/data/sample_libsvm_data.txt")

    val sss =data.take(1)
    sss.foreach(x=>println(x.label,x.features))
    //data.take(10).foreach(println)

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)



    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    scoreAndLabels.foreach(println)
//
//    // Get evaluation metrics.
//    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
//    scoreAndLabels.foreach(println)
//    val auROC = metrics.areaUnderROC()
//
//    println("Area under ROC = " + auROC)
//
//    // Save and load model
////    model.save(sc, "myModelPath")
////    val sameModel = SVMModel.load(sc, "myModelPath")
  }

}
