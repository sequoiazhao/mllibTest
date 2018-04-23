package com.MLTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * @author zhaoming on 2018-03-15 17:24
  **/
object TestGBT1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.FATAL)
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val data = MLUtils.loadLibSVMFile(sc, "D:\\data\\sample_libsvm_data.txt")
    data.toDF("label","features").show()




    val data2 = sqlContext.read.format("libsvm").load("D:\\data\\sample_libsvm_data.txt")
    data2.show()


//    labeledPoint
    //data.foreach(println)
    val splits =data.randomSplit(Array(0.7,0.3))
    val (trainingData,testData)=(splits(0),splits(1))


    val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.numIterations = 10// Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.maxDepth = 15
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

    val labelsAndPredictions = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
    println("Test Mean Squared Error = " + testMSE)
    println("Learned regression GBT model:\n" + model.toDebugString)
    //println(model.totalNumNodes)



   val tree =  model.trees

    println(tree.apply(0).topNode.getClass)
      tree.zipWithIndex.map { case (tree, treeIndex) =>

      s"  Tree $treeIndex:\n" + tree.topNode
    }.fold("")(_ + _)




  }

}
