package com.AllTest.mllibLDA

import java.io.File

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.clustering._
import org.apache.spark.rdd.RDD

/**
  * LDA算法工具类
  *
  * @author zhaoming on 2017-12-07 17:08
  **/
class LDAUtils(private var k: Int,
               private var maxIterations: Int,
               private var algorithm: String,
               private var alpha: Double,
               private var beta: Double,
               private var checkpointInterval: Int,
               private var checkpointDir: String
              ) extends Logging with Serializable {

  def this() = this(10, 20, "em", -1, -1, 10, "")

  //set 和 get
  def setK(k: Int): this.type = {
    require(k > 0, "主题个数K必须大于0")
    this.k = k
    this
  }

  def setMaxIterations(maxIterations: Int): this.type = {
    require(maxIterations > 0, "迭代次数必须大于0")
    this.maxIterations = maxIterations
    this
  }

  def setAlgorithm(algorithm: String): this.type = {
    require(algorithm.equalsIgnoreCase("em") || algorithm.equalsIgnoreCase("online"), "参数估计算法只支持：em/online")
    this.algorithm = algorithm
    this
  }

  def setAlpha(alpha: Double): this.type = {
    this.alpha = alpha
    this
  }

  def setBeta(beta: Double): this.type = {
    this.beta = beta
    this
  }

  def setCheckpointInterval(checkpointInterval: Int): this.type = {
    require(checkpointInterval > 0, "检查点间隔必须大于0")
    this.checkpointInterval = checkpointInterval
    this
  }

  def setCheckpointDir(checkpointDir: String): this.type = {
    this.checkpointDir = checkpointDir
    this
  }

  def getK: Int = this.k

  def getMaxIterations: Int = this.maxIterations

  def getAlgorithm: String = this.algorithm

  def getAlpha: Double = this.alpha

  def getBeta: Double = this.beta

  def getCheckpointInterval: Int = checkpointInterval

  def getCheckpointDir: String = this.checkpointDir

  //选择算法em或Online

  private def selectOptimizer(algorithm: String, corpusSize: Long): LDAOptimizer = {

    val optimizer = algorithm.toLowerCase match {
      case "em" => new EMLDAOptimizer
      case "online" => new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0 / corpusSize)
      case _ => throw new IllegalArgumentException(
        s"只支持：em和online算法，输入是：$algorithm.")
    }
    optimizer
  }

  def train(data: RDD[(Long, Vector)]): LDAModel = {
    val sc = data.sparkContext

    if (checkpointDir.nonEmpty) {
      sc.setCheckpointDir(checkpointDir)
    }

    val actualCorpusSize = data.map(_._2.numActives).sum().toLong
    val optimizer = selectOptimizer(algorithm, actualCorpusSize)

    val lda = new LDA()
      .setK(k)
      .setOptimizer(optimizer)
      .setMaxIterations(maxIterations)
      .setDocConcentration(alpha)
      .setTopicConcentration(beta)
      .setCheckpointInterval(checkpointInterval)

    //训练LDA模型
    val trainStart = System.nanoTime()
    val ldaModel = lda.run(data)
    val trainElapsed = (System.nanoTime() - trainStart) / 1e9

    trainInfo(data, ldaModel, trainElapsed)
    ldaModel

  }

  //打印训练相关信息
  def trainInfo(data: RDD[(Long, Vector)], ldaModel: LDAModel, trainElapsed: Double) = {
    println("完成LDA模型训练！")
    println(s"\t训练时长：$trainElapsed sec")

    val actualCorpusSize = data.map(_._2.numActives).sum().toLong

    ldaModel match {
      case distLDAModel: DistributedLDAModel =>
        val avgLogLikelihood = distLDAModel.logLikelihood / actualCorpusSize.toDouble
        val logPerplexity = distLDAModel.logPrior
        println(s"\t训练数据平均对数似然度:$avgLogLikelihood")
        println(s"\t训练数据对数困惑度：$logPerplexity")
        println()

      case localLDAModel: LocalLDAModel =>
        val avgLogLikelihood = localLDAModel.logLikelihood(data) / actualCorpusSize.toDouble
        val logPerplexity = localLDAModel.logPerplexity(data)
        println(s"\t 训练数据平均对数似然度：$avgLogLikelihood")
        println(s"\t 训练数据对数困惑度：$logPerplexity")
        println()
      case _ =>

    }
  }

  //lda新文档预测
  def predict(data: RDD[(Long, Vector)], ldaModel: LDAModel, cvModel: CountVectorizerModel, sorted: Boolean = false)
  : (RDD[(Long, Array[(Double, Int)])], Array[Array[(String, Double)]]) = {
    val vocabArray = cvModel.vocabulary

    var docTopics: RDD[(Long, Array[(Double, Int)])] = null

    if (sorted) {
      docTopics = getSortedDocTopics(data, ldaModel, sorted)
    } else {
      docTopics = getDocTopics(ldaModel, data).map(doc => (doc._1, doc._2.toArray.zipWithIndex))
    }

    val topicWords: Array[Array[(String, Double)]] = getTopicWords(ldaModel, vocabArray)

    (docTopics, topicWords)

  }

  //主题描述，包括主题下每个词和权重
  def getTopicWords(ldaModel: LDAModel, vocabArray: Array[String]): Array[Array[(String, Double)]] = {
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 20)

    topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
    }
  }

  //文档-主题分布结果
  //(docID,topicDistributions)

  def getDocTopics(ldaModel: LDAModel, corpus: RDD[(Long, Vector)]): RDD[(Long, Vector)] = {
    var topicDistributions: RDD[(Long, Vector)] = null
    ldaModel match {
      case distLDAModel: DistributedLDAModel =>
        topicDistributions = distLDAModel.toLocal.topicDistributions(corpus)
      case localLDAModel: LocalLDAModel =>
        topicDistributions = localLDAModel.topicDistributions(corpus)
      case _ =>
    }

    topicDistributions
  }

  //排序后的模型
  def getSortedDocTopics(corpus: RDD[(Long, Vector)], ldaModel: LDAModel, desc: Boolean = true): RDD[(Long, Array[(Double, Int)])] = {
    var topicDistributions: RDD[(Long, Vector)] = null
    ldaModel match {
      case distLDAModel: DistributedLDAModel =>
        topicDistributions = distLDAModel.toLocal.topicDistributions(corpus)
      case localLDAModel: LocalLDAModel =>
        topicDistributions = localLDAModel.topicDistributions(corpus)
      case _ =>
    }

    val indexedDist = topicDistributions.map(doc => (doc._1, doc._2.toArray.zipWithIndex))
    if (desc) {
      indexedDist.map(doc => (doc._1, doc._2.sortWith(_._1 > _._1)))
    } else {
      indexedDist.map(doc => (doc._1, doc._2.sortWith(_._1 < _._1)))
    }
  }

  //save
  def save(sc: SparkContext, modelPath: String, ldaModel: LDAModel): Unit = {
    ldaModel match {
      case distModel: DistributedLDAModel =>
        distModel.toLocal.save(sc, modelPath + File.separator + "model")
      case localModel: LocalLDAModel =>
        localModel.save(sc, modelPath + File.separator + "model")
      case _ =>
        println("保存模型出错")
    }
  }

  //load
  def load(sc: SparkContext, modelPath: String): LDAModel = {
    val lDAModel = LocalLDAModel.load(sc, modelPath + File.separator + "model")
    lDAModel
  }

}






























