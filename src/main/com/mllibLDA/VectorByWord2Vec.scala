package com.mllibLDA
import java.io._

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
/**
  * @author zhaoming on 2018-01-09 13:38
  **/
class VectorByWord2Vec
(
  private  var minDocFreq:Int,
  private var vocabSize:Int
){

}
