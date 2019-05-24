package com.AllTest.hivemall

//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.ml.feature.HmLabeledPoint
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.hive.HivemallOps._
//import org.apache.spark.sql.hive.HivemallUtils._
//import org.apache.spark.sql.types.{DoubleType, IntegerType}


/**
  * @author zhaoming on 2018-02-08 15:51
  **/
object TestMall {
  //def main(args: Array[String]): Unit = {

//    Logger.getLogger("org").setLevel(Level.FATAL)
//    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
//      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
//    val sc = new SparkContext(conf)
//
//    val sqlhiveContext = new HiveContext(sc)
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//
//
//    val trainRdd = sc.textFile("d:/code_test/mllibtest/data/a9a.train")
//      .map(HmLabeledPoint.parse)
//    trainRdd.foreach(println)
//
//    // Create the DataFrame that has exactly 2 partitions and
//    // amplify the data by 3 times.
//    val trainDf = sqlContext.createDataFrame(trainRdd)
//      //.coalesce(2).part_amplify(3)
//
//   // trainDf.show()
//
//
//
////    val testDf = sqlContext.createDataFrame(trainRdd)
////      .select("label".as("target"), ft2vec("features").as("features"))
//
//    // Make a model from the training data
//    import sqlContext.implicits._
//    val model = trainDf
//      .train_logregr($"features", $"label", "-total_steps 32561")
//      .groupby("feature").agg("weight" -> "avg")
//      .as("feature", "weight")
//
//    model.show()
//
//    val model2 = trainDf.train_arow_regr($"features", $"label", "-total_steps 32561")
//      .groupby("feature").agg("weight" -> "avg")
//      .as("feature", "weight")
//
//   // val modelUdf = org.apache.spark.sql.hive.HivemallUtils.funcModel(model)
//    //val moed = funcModel(model)
//    //model.show()

 // }

}
