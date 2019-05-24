package com.Common

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhaoming on 2018-07-25 19:14
  **/
class GetConf {
  def getConf: (SparkContext, HiveContext) = {
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val myContext = new HiveContext(sc)
    (sc, myContext)
  }

  def getConfSingle: SparkContext = {
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    sc
  }
}
