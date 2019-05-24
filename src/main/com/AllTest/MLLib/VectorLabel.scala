package com.AllTest.MLLib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * @author zhaoming on 2018-08-10 17:17
  **/
object VectorLabel {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.FATAL)
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)

    val vd = Vectors.dense(2, 0, 6)
    val pos =LabeledPoint(1,vd)
    println(pos)
  }
}
