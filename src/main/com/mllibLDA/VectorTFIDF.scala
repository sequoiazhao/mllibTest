package com.mllibLDA

import java.io.Serializable

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * @author zhaoming on 2018-01-09 13:55
  **/
class VectorTFIDF(
                   private var minDocFreq: Int,
                   private var vocabSize: Int,
                   private var toTFIDF: Boolean
                 ) extends Serializable {
  def this() = this(minDocFreq = 1, vocabSize = 5000, toTFIDF = true)

  def setMinDocFreq(minDocFreq: Int): this.type = {
    require(minDocFreq > 0, "最小文档频率必须大于0")
    this.minDocFreq = minDocFreq
    this
  }

  def setVocabSize(vocabSize: Int): this.type = {
    require(vocabSize > 1000, "词汇表大小不小于1000")
    this.vocabSize = vocabSize
    this
  }

  def setToTFIDF(toTFIDF: Boolean): this.type = {
    this.toTFIDF = toTFIDF
    this
  }

  def getMinDocFreq: Int = this.minDocFreq

  def getVocabSize: Int = this.vocabSize

  def getToTFIDF: Boolean = this.toTFIDF


    def vectorize(tokenDF:DataFrame):Unit={//(RDD[LabeledPoint], CountVectorizerModel, Vector)    ={
      //生成CountVectorizer
      val cvModel = new CountVectorizer()
        .setInputCol("tokens")
        .setOutputCol("features")
        .setVocabSize(vocabSize)
        .fit(tokenDF)
    }
















  


}
