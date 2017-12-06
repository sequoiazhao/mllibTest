package com.mllibtest

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.mllib.feature.{IDF, IDFModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
  * @author zhaoming on 2017-12-05 15:49
  **/
class Vectorizer(
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

  //生成词向量模型
  def genCvModel(df: DataFrame, vocabSize: Int): CountVectorizerModel = {
    val cvModel = new CountVectorizer()
      .setInputCol("tokens")
      .setOutputCol("features")
      .setVocabSize(vocabSize)
      .fit(df)

    cvModel
  }

  //词频数据转化成特征LabeledPoint
  def toTFLP(df: DataFrame, cvModel: CountVectorizerModel): RDD[LabeledPoint] = {
    val documents = cvModel.transform(df)
      .select("id", "features")
      .map { case Row(id: Long, features: Vector) => LabeledPoint(id, features) }

    documents
  }

  //根据特征向量生成tf-idf模型
  def genIDFModel(documents:RDD[LabeledPoint]):IDFModel = {
    val features = documents.map(_.features)

    val idf = new IDF(minDocFreq)
    idf.fit(features)
  }

  //将词频LabeledPoint转化为TF-IDF的LabeledPoint
 def toTFIDFLP(documents:RDD[LabeledPoint],idfModel:IDFModel):RDD[LabeledPoint]={
    val ids = documents.map(_.label)
   val allFeatures = documents.map(_.features)

   val  tfidf =idfModel.transform(allFeatures)

   val tfidfDocs = ids.zip(tfidf).map{case (id,features)=>LabeledPoint(id,features)}
   tfidfDocs
  }

  //已经分词中文数据向量化
  def vectorize(data:RDD[(Long,scala.Seq[String])]):(RDD[LabeledPoint],CountVectorizerModel,Vector)={
    val sc =data.context
    val sqlContext =SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val tokenDF =data.toDF("id","tokens")

    var startTime = System.nanoTime()

    //生成cvModle

  }



}





































