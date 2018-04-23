package com.mllibLDA

import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
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

object VectorTFIDF {

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
    //    val tokenDF = tokenDFx.withColumn("id", monotonically_increasing_id()).limit(10)
    //      .drop("idx")

    val tokenDF = tokenDFx.withColumn("id", lit(row_number() over Window.orderBy("tokens")).cast(LongType)).limit(20)
      .drop("idx")

    println("输入数据的长度" + tokenDF.count() + "开始TF-IDF转换")





    //////////////////////////开始进行数据的处理===================

    //1、totf--cvModel，为了可以找到原来的词，采用CountVectorizer()
    val cvModel = new CountVectorizer()
      .setInputCol("tokens")
      .setOutputCol("features")
      .setVocabSize(4000)
      .fit(tokenDF)

    val documents = cvModel.transform(tokenDF)
      .select("id", "features")
      .map { case Row(id: Long, features: Vector) => LabeledPoint(id, features) }
    documents.foreach(println)

    //2、=============调用自己的IDFtoidf--IDFModel 设定最小文档频率

    //    val ids = documents.map(_.label)
    //    val features = documents.map(_.features)
    //
    //    val idfModelold: IDFModel = new IDF(2)
    //      .fit(features)
    //
    //    val tfidf = idfModelold.transform(features)
    //
    //    val tfidfDocs = ids.zip(tfidf).map { case (id, features) => LabeledPoint(id, features) }

    // tfidfDocs.foreach(n => println(n.features))
    //================================================


    //3、定义标准IDF，逆词频，进行计算

    val doc2 = cvModel.transform(tokenDF).select("id", "features")
      .withColumnRenamed("features", "rawfeatures")

    val idfc = new org.apache.spark.ml.feature.IDF()
      .setInputCol("rawfeatures")
      .setOutputCol("features")
      .setMinDocFreq(2)


    val idfModel = idfc.fit(doc2)


    val documentsIDF = idfModel.transform(doc2)
      .select("id", "features")
      .map { case Row(id: Long, features: Vector) => LabeledPoint(id, features) }


    //获得三个需要返回的量 val (vectorizedRDD, cvModle, idf)    //cvModel
    //documents 是TF，tfidfDocs是TF-IDF
    val vectorizedRDD = documentsIDF
    val idf = idfModel.idf

    //4、LDA 训练

    val k = 10 //主题的个数
    val analysisType = "em" //参数估计
    val maxIterations = 30 //迭代次数
    val alpha = -1
    val beta = -1
    val checkpointInterval = 10
    val checkpointDir = ""


    //5、数据类型的转换
    val trainRDD = vectorizedRDD.map(line => (line.label.toLong, line.features))

    val actualCorpusSize = trainRDD.map(_._2.numActives).sum().toLong
    //val sc2 = trainRDD.sparkContext

    //set optimizer
    // trainRDD.foreach(x=>println(x._2.numActives,x._2.toArray.length))

    //==================其他配置==================
    //    val actualCorpusSize = trainRDD.map(_._2.numActives).sum().toLong
    //    println(actualCorpusSize)
    //    val str ="em"
    //    val optimizer =str.toLowerCase match {
    //      case "em"=> new EMLDAOptimizer
    //      case "online"=> new OnlineLDAOptimizer().setMiniBatchFraction(0.05+1.0/actualCorpusSize)
    //      case _=> throw new IllegalArgumentException(
    //        s"错误的输入$str"
    //      )
    //    }
    //==========================================

    val algorithm = new EMLDAOptimizer //取em估计

    //val algorithm =new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0 / actualCorpusSize)

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

    //7、=============predict==============


    val sorted = false
    val testRDD = trainRDD

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

    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 25)
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
    //datax.show()

    val joinData = datax.join(tokenDF, Seq("id"))

    joinData.show(false)

  }

}

//======================老的输出模型======================
//    val ldaUtils = new LDAUtils()
//      .setK(k)
//      .setAlgorithm(analysisType)
//      .setMaxIterations(maxIterations)
//
//    //val ldaModel: LDAModel = ldaUtils.train(trainRDD)
//
//    val (docTopics, topicWords) = ldaUtils.predict(trainRDD, ldaModel, cvModel, sorted = true)
//
//    //======输出训练的结果
//
//
//    //docTopics.take(1).foreach(_._2.map(ss=>println(ss._2,ss._1)))
//
//    //重新排序后输出
//    val tst = docTopics.take(1).foreach { x =>
//      val s = x._2.sortBy(_._2)
//      s.foreach(println)
//    }

//.foreach(_._2.map(ss=>println(ss._2,ss._1)))
//topicWords.apply(2).foreach(println)
//======================================================


//================典型模型取法====by spark sample================
//    val vocabArray = cvModel.vocabulary
//    println(vocabArray.length)
//
//    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)  //显示词的长度
//
//    val topics = topicIndices.map { case (terms, termWeights) =>
//      terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
//    }
//    println(s" topics:")
//    topics.zipWithIndex.foreach { case (topic, i) =>
//      println(s"TOPIC $i")
//      topic.foreach { case (term, weight) =>
//        println(s"$term\t$weight")
//      }
//      println()
//    }
//================典型模型取法====================


