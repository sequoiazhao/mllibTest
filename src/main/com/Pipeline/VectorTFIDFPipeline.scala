package com.Pipeline

import com.mllibLDA.PreUtils
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhaoming on 2018-01-09 13:55
  **/

object VectorTFIDFPipeline {

  def main(args: Array[String]): Unit = {
    //spark
    Logger.getLogger("org").setLevel(Level.FATAL)
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val sqlcon: SQLContext = new HiveContext(sc)

    //读入文件
    val hadoopConf = sc.hadoopConfiguration
    val inPath = "D:/code_test/mllibtest/data/train"
    val fs = new Path(inPath).getFileSystem(hadoopConf)
    val len = fs.getContentSummary(new Path(inPath)).getLength / (1024 * 1024)
    val minPart = (len / 32).toInt

    //数据标记
    val data = sc.textFile(inPath, minPart).zipWithIndex().map(_.swap)

    println(data.getClass)

    //分词
    val resultRDD = new PreUtils().run(data)

    val vecModelPath = "D:/code_test/mllibtest/model"
    val ldaModelPath = "D:/code_test/mllibtest/model/ldaModel"


    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val tokenDFx = resultRDD.toDF("idx", "tokens")

    val tokenDF = tokenDFx.withColumn("id", lit(row_number() over Window.orderBy("tokens")).cast(LongType)).limit(20)
      .drop("idx")

    println("输入数据的长度" + tokenDF.count() + "开始TF-IDF转换")


    //======tf-idf pipeline======================

    val cv = new CountVectorizer()
      .setVocabSize(4000)
      .setInputCol("tokens")
      .setOutputCol("rawfeatures")

    val idfc = new org.apache.spark.ml.feature.IDF()
      .setMinDocFreq(2)
      .setInputCol(cv.getOutputCol)
      .setOutputCol("features")

    val pipeline = new Pipeline().setStages(Array(cv, idfc))

    val model = pipeline.fit(tokenDF)
    val documentsIDF2 = model.transform(tokenDF)



    val trainRDD = documentsIDF2.select("id", "features")
      .map { case Row(id: Long, features: Vector) => LabeledPoint(id, features) }
      .map(line => (line.label.toLong, line.features))

    val actualCorpusSize = trainRDD.map(_._2.numActives).sum().toLong

    //LDA 训练

    val k = 10 //主题的个数
    val analysisType = "em" //参数估计
    val maxIterations = 30 //迭代次数
    val alpha = -1
    val beta = -1
    val checkpointInterval = 10
    val checkpointDir = ""

    val algorithm = new EMLDAOptimizer //取em估计

    val lda = new LDA()
      .setK(k)
      .setOptimizer(algorithm)
      .setMaxIterations(maxIterations)
      .setDocConcentration(alpha)
      .setTopicConcentration(beta)
      .setCheckpointInterval(checkpointInterval)


    val ldaModel: LDAModel = lda.run(trainRDD)


    //=================trainInfo=================
    ldaModel match {
      case distLDAModel: DistributedLDAModel =>
        val avgLogLikelihood = distLDAModel.logLikelihood / actualCorpusSize.toDouble
        val logPerplexity = distLDAModel.logPrior
        println(s"\t分布式训练数据平均对数似然度:$avgLogLikelihood")
        println(s"\t分布式训练数据对数困惑度：$logPerplexity")
        println()
      case localLDAModel: LocalLDAModel =>
        val avgLogLikelihood = localLDAModel.logLikelihood(trainRDD) / actualCorpusSize.toDouble
        val logPerplexity = localLDAModel.logPerplexity(trainRDD)
        println(s"\t 本地训练数据平均对数似然度：$avgLogLikelihood")
        println(s"\t 本地训练数据对数困惑度：$logPerplexity")
        println()
      case _ =>

    }

    //=============predict==============

    val sorted = false
    val testRDD = trainRDD
    val cvModel =cv.fit(tokenDF)

    //var docTopics: RDD[(Long, Array[(Double, Int)])] = null

    var topicDIstributions: RDD[(Long, Vector)] = null
    // if (sorted) {
    ldaModel match {
      case disLDAModel: DistributedLDAModel =>
        topicDIstributions = disLDAModel.toLocal.topicDistributions(testRDD)
      case localLDAModel: LocalLDAModel =>
        topicDIstributions = localLDAModel.topicDistributions(testRDD)
      case _ =>
    }

    val indexedDist = topicDIstributions.map(doc => (doc._1, doc._2.toArray.zipWithIndex))

    //===================是否排序============================
    //    var inde1: RDD[(Long, Array[(Double, Int)])] = null
    //
    //    if (sorted) {
    //      inde1 = indexedDist.map(doc => (doc._1, doc._2.sortWith(_._1 > _._1))) //倒序
    //    } else {
    //      inde1 = indexedDist.map(doc => (doc._1, doc._2.sortWith(_._1 < _._1))) //正序
    //    }
    //======================================================

    val docTopics = indexedDist //文章在topic上的得分 RDD

    //indexedDist.take(1).foreach(doc => (doc._1, doc._2.foreach(println)))

    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 20)
    val topicWords = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (cvModel.vocabulary(term.toInt), weight) }
    }

    println(s" topics:")
    topicWords.zipWithIndex.foreach { case (topic, i) =>
      println(s"TOPIC $i")
      topic.foreach { case (term, weight) =>
        println(s"$term\t$weight")
      }
      println()
    }

    // topicWords.take(1).foreach(doc => doc.foreach(x => println(x._1, x._2))) //某个topic词袋中的词 Array

    //=========================归类文章到DataFrame=================

    val tesp2 = docTopics.map(doc => {
      val temp = doc._2.filter(_._1 > 0.1)

      val docResult = (doc._1, temp.map(_._2), temp.map(_._1), topicWords.apply(doc._2.max._2).map(_._1))
      docResult
    })

    val datax = tesp2.toDF("id", "topic", "score", "word")

    val joinData = datax.join(tokenDF, Seq("id"))

    joinData.show(false)

  }

}



