package com.MLToolsTest.otherALS

//import org.apache.spark.sql.Row
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.types.{StringType, StructField, StructType}
//import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhaoming on 2017-12-01 12:55
  **/
object TestUDF {

//  def main(args: Array[String]): Unit = {
//
//    val sparkConf = new SparkConf().setAppName("User mining").setMaster("local[*]")
//      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
//
//    val sc = new SparkContext(sparkConf)
//    val sqlContext = new HiveContext(sc)
//
//    val bigData = Array("Spark", "Spark", "Hadoop", "Spark", "Hadoop", "Spark", "Spark", "Hadoop", "Spark", "Hadoop")
//
//    val bigDataRDD = sc.parallelize(bigData)
//
//    val bigDataRDDRow = bigDataRDD.map(item => Row(item))
//
//    val structType = StructType(Array(StructField("word", StringType, true)))
//
//    val bigDataDF = sqlContext.createDataFrame(bigDataRDDRow,structType)
//    //bigDataDF.show()
//
//    bigDataDF.registerTempTable("bigDataTable")
//
//    sqlContext.udf.register("computeLength",(input:String)=>input.length)
//    sqlContext.sql("select word, computeLength(word) as length from bigDataTable").show
//
//    sqlContext.udf.register("wordCount",new MyUDAF)
//
//    sqlContext.sql("select word, wordCount(word) as count, computeLength(word) as length from bigDataTable group by word").show






 // }





}
































