package com.AllTest.deeplearning4j

import org.apache.spark.sql.functions._
import org.deeplearning4j.datasets.iterator.impl.MnistDataSetIterator
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator
import java.util.zip.GZIPInputStream
import java.io.FileInputStream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.deeplearning4j.spark.impl.paramavg.ParameterAveragingTrainingMaster
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer


/**
  * @author zhaoming on 2018-02-02 17:38
  **/
object TestFirst {
  def main(args: Array[String]): Unit = {
//    val nChannels = 1 //black & white picture, 3 if color image
//    val outputNum = 10 //number of classification
//    val batchSize = 64 //mini batch size for sgd
//    val nEpochs = 10 //total rounds of training
//    val iterations = 1 //number of iteration in each traning round
//    val seed = 123 //random seed for initialize weights
//
//    var mnistTrain: DataSetIterator = null
//    var mnistTest: DataSetIterator = null
//    mnistTrain = new MnistDataSetIterator(batchSize, true, 12345)
//    mnistTest = new MnistDataSetIterator(batchSize, false, 12345)
//
//    println(mnistTrain.numExamples())

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val MyContext = new HiveContext(sc)

    val examplesPerDataSetObject = 1
//    val trainingMaster = new ParameterAveragigTrainingMaster.Builder(examplesPerDataSetObject)
//      .build()

    //val sparkNetwork = new SparkDl4jMultiLayer(sc, networkConfig, trainingMaster)
  }

}
