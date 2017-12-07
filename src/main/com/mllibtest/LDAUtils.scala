package com.mllibtest

import org.apache.spark.Logging
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
    val trainElapsed =(System.nanoTime()-trainStart)/1e9

    trainInfo(data,ldaModel,trainElapsed)
    ldaModel

  }

  //打印训练相关信息
  def trainInfo(data:RDD[(Long,Vector)],ldaModel:LDAModel,trainElapsed:Double)={
    println("完成LDA模型训练！")
    println(s"\t 训练时长：$trainElapsed sec")

    val actualCorpusSize = data.map(_._2.numActives).sum().toLong

    ldaModel match{
      case distLDAModel:DistributedLDAModel=>
        val avgLogLikelihood = distLDAModel.logLikelihood / actualCorpusSize.toDouble
        val logPerplexity = distLDAModel.logPrior
        println(s"\t训练数据平均对数似然度:$avgLogLikelihood")
        println(s"\t训练数据对数困惑度：$logPerplexity")
        println()

      case localLDAModel:LocalLDAModel=>
        val avgLogLikelihood = localLDAModel.logLikelihood(data)/actualCorpusSize.toDouble
        val logPerplexity = localLDAModel.logPerplexity(data)
        println(s"\t 训练数据平均对数似然度：$avgLogLikelihood")
        println(s"\t 训练数据对数困惑度：$logPerplexity")
        println()
      case _=>

    }
  }

  //lda新文档预测
  def predict(data:RDD[(Long,Vector)],ldaModel:LDAModel,cvModel:CountVectorizerModel,sorted:Boolean=false)
  :(RDD[(Long,Array[(Double,Int)])],Array[Array[(String,Double)]])={

  }


}






























