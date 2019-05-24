package com.MLToolsTest.otherALS

import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors

/**
  * @author zhaoming on 2017-11-29 11:10
  **/
object MllibTest2 {
//  val conf = new SparkConf()
//    .setMaster("local")
//    .setAppName("testSummary")
//
//  val sc = new SparkContext(conf)
def main(args: Array[String]): Unit = {
  val vd = Vectors.dense(1, 2, 3, 4, 5)
  val vdResult = Statistics.chiSqTest(vd)
  println(vdResult)


  val mtx = Matrices.dense(3, 2, Array(1, 3, 5, 2, 4, 6))
  val mtxResult = Statistics.chiSqTest(mtx)
  println(mtxResult)
}


}
