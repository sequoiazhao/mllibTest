package com.AllTest.MllibAlgorithmDocTest

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhaoming on 2018-05-29 17:10
  **/
object NaiveBayesTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")

    val sc = new SparkContext(conf)

    val data = MLUtils.loadLibSVMFile(sc, "d:/data/sample_libsvm_data.txt")

    val Array(training, test) = data.randomSplit(Array(0.6, 0.4))
    //training.foreach(println)


    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

    println(accuracy)

    predictionAndLabel.foreach(println)
    // $example off$

    sc.stop()
  }

}
