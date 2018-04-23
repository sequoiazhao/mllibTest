package com.SVMWithSGD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

/**
  * @author zhaoming on 2018-02-05 14:00
  **/
object TestSVMWithSGD {
  def main(args: Array[String]): Unit = {
    //spark
    Logger.getLogger("org").setLevel(Level.FATAL)
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val sqlconf: SQLContext = new HiveContext(sc)


    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "d:/data/sample_libsvm_data2.txt")
    data.foreach(println)

    //LabeledPoint
//    val pos = LabeledPoint(1.0,Vectors.dense(1.0,0.0,3.0))
//    println(pos)
//
//    val vs = Vectors.sparse(4,Array(0,1,2,3),Array(9,3,4,2))
//
//    val p2 = LabeledPoint(2.0,vs)
//    println(p2)


    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    val numIterations = 100
    val model = SVMWithSGD.train(training,numIterations)

    model.clearThreshold()

    val scoreAndLabels = test.map{point=>
      val score = model.predict(point.features)
      (score,point.label)
    }


    val metrics = new  BinaryClassificationMetrics(scoreAndLabels)
    val auRoc = metrics.areaUnderROC()

    println("Area under ROC = " + auRoc)
  }

}
