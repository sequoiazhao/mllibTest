package com.mllibLDA

import java.io._

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.reflect.io.File

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
  def genIDFModel(documents: RDD[LabeledPoint]): IDFModel = {
    val features = documents.map(_.features)

    val idf = new IDF(minDocFreq)
    idf.fit(features)
  }

  //将词频LabeledPoint转化为TF-IDF的LabeledPoint
  def toTFIDFLP(documents: RDD[LabeledPoint], idfModel: IDFModel): RDD[LabeledPoint] = {
    val ids = documents.map(_.label)
    val allFeatures = documents.map(_.features)

    val tfidf = idfModel.transform(allFeatures)

    val tfidfDocs = ids.zip(tfidf).map { case (id, features) => LabeledPoint(id, features) }

    tfidfDocs
  }

  //已经分词中文数据向量化
  def vectorize(tokenDF: DataFrame): (RDD[LabeledPoint], CountVectorizerModel, Vector) = {
    //val sc = data.context
    //val sqlContext = SQLContext.getOrCreate(sc)
    // import sqlContext.implicits._

    //val tokenDF = data.toDF("id", "tokens")
    //可以在这里对接数据库组织数据
    //tokenDF.show()

    var startTime = System.nanoTime()

    //生成cvModle
    val cvModel = genCvModel(tokenDF, vocabSize)
    val cvTime = (System.nanoTime() - startTime) / 1e9
    startTime = System.nanoTime()
    println(s"start cvModel!\n\t time :$cvTime sec\n")


    //changed to LabeledPoint
    var tokensLP = toTFLP(tokenDF, cvModel)
    val lpTime = (System.nanoTime() - startTime) / 1e9
    startTime = System.nanoTime()
    println(s"change LabeledPoint!\n\t time :$lpTime sec\n")
    // tokensLP.foreach(println)

    //changed to TFDF
    var idfModel: IDFModel = null
    if (toTFIDF) {
      //create idfModel
      idfModel = genIDFModel(tokensLP)
      tokensLP = toTFIDFLP(tokensLP, idfModel)
    }
    // tokensLP.foreach(println)
    val idfTime = (System.nanoTime() - startTime) / 1e9
    println(s"change TFIDF end!\n\t time :$idfTime sec\n")
    (tokensLP, cvModel, idfModel.idf)

  }


  //计算时候用的
  def vectorize(tokenDF: DataFrame, cvModel: CountVectorizerModel, idf: Vector): RDD[LabeledPoint] = {
    // val sc = data.context
    //  val sqlContext = SQLContext.getOrCreate(sc)


    //tokenDF.show()

    //转化为LabeledPoint
    var tokensLP = toTFLP(tokenDF, cvModel)

    if (toTFIDF) {
      val idfModel = new IDFModel(idf)
      tokensLP = toTFIDFLP(tokensLP, idfModel)
    }

    tokensLP
  }


  //save idf and cvModel
  def save(modelPath: String, cvModel: CountVectorizerModel, idf: Vector): Unit = {
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(modelPath + File.separator + "IDF")))
    bw.write(idf.toArray.mkString(","))
    cvModel.save(modelPath + File.separator + "cvModel")
    bw.close()
  }


  //load
  def load(modelPath: String): (CountVectorizerModel, Vector) = {
    val br = new BufferedReader(new InputStreamReader(new FileInputStream(modelPath + File.separator + "IDF")))
    val idfArray = br.readLine().split(",").map(_.toDouble)
    val idf = Vectors.dense(idfArray)
    val cvModel = CountVectorizerModel.load(modelPath + File.separator + "cvModel")

    br.close()
    (cvModel, idf)
  }


}






























