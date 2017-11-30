package com.mllibtest

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhaoming on 2017-11-29 19:15
  **/
object CollaborativeFiltering {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("testSummary")

    val sc = new SparkContext(conf)

    val users = sc.parallelize(Array("aaa", "bbb", "ccc", "ddd", "eee"))

    val films = sc.parallelize(Array("smzdm", "ylxb", "znh", "nhsc", "fcwr"))


  }

}
