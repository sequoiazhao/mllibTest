package com.mllibLDA


import java.util

import org.ansj.domain.Term
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.DicAnalysis
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * @author zhaoming on 2017-12-08 9:48
  **/
object LDATestDemo {

  def main(args: Array[String]): Unit = {
    //spark

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)

    val dataPath = "D:/code/mllibtest/data/test"
    val vecModelPath = "D:/code/mllibtest/model"
    val ldaModelPath = "D:/code/mllibtest/model/ldaModel"

    //读入文件
    val hadoopConf = sc.hadoopConfiguration
    val fs = new Path(dataPath).getFileSystem(hadoopConf)
    val len = fs.getContentSummary(new Path(dataPath)).getLength / (1024 * 1024)
    val minPart = (len / 32).toInt

    //数据标记
    val data = sc.textFile(dataPath, minPart).zipWithIndex().map(_.swap)


    //数据清洗
    val cleanedRDD = data.map(str => (str._1, baseClean(str._2)))

    //    cleanedRDD.foreach(println)
    //去除符号，增加新的停用词

    val resultRDD = cleanedRDD.map { line =>
      (line._1, wordSegment(line._2))
    }.filter(_._2.nonEmpty).map(line => (line._1, line._2.get))


    //向量化
    val minDocFreq = 2 //最小文档频率阈值
    val toTFIDF = true //是否将TF转化为TF-IDF
    val vocabSize = 2000 //词汇表大小

    val vectorizer = new Vectorizer()
      .setMinDocFreq(minDocFreq)
      .setToTFIDF(toTFIDF)
      .setVocabSize(vocabSize)

    val (cvModel, idf) = vectorizer.load(vecModelPath)
    val vectorizedRDD = vectorizer.vectorize(resultRDD, cvModel, idf)

    val testRDD = vectorizedRDD.map(line => (line.label.toLong, line.features))


    //--加载LDA模型
    val k = 10
    val analysisType = "em" //参数估计算法
    val maxIterations = 20 //迭代次数

    val ldaUtils = new LDAUtils()
      .setK(k)
      .setAlgorithm(analysisType)
      .setMaxIterations(maxIterations)

    val ldaModel = ldaUtils.load(sc, ldaModelPath)
    val (docTopics, topicWords) = ldaUtils.predict(testRDD, ldaModel, cvModel, sorted = true)


    //输出结果
    println("文档-主题分布:")
    println(docTopics)
    docTopics.take(3).foreach(doc => {
      val docTopicsArray = doc._2.map(topic => topic._2 + " : " + topic._1)
      println(doc._1 + ":[" + docTopicsArray.mkString(" , ") + "]")
    })

    println("主题-词：")
    println(topicWords.length)
    topicWords.take(10).zipWithIndex.foreach(topic => {
      println("Topic: " + topic._2)
      topic._1.foreach(word => {
        println(word._1 + "\t" + word._2)
      })
    })

    //保存结果应该重新组成DataFrame

    println("end")

  }

  private val baseExpr =
    """[^\w-\s+\u4e00-\u9fa5]""".r //匹配英文字母、数字、中文汉字之外的字符

  private val enExpr = "[A-Za-z]+".r

  def baseClean(line: String): String = {
    var result = line.trim
    //val numToChar = "数"
    //val f2j = false

    //去除不可见字符
    result = baseExpr.replaceAllIn(result, "")
    result = StringUtils.trimToEmpty(result)

    //去除英文字符
    result = enExpr.replaceAllIn(result, "")

    result
  }

  def ansjSegment(text: String): util.List[Term] = {
    val filter = new StopRecognition()
    filter.insertStopNatures("w")
    filter.insertStopWords("的")

    val result = DicAnalysis.parse(text).recognition(filter)
    result.getTerms
  }


  def wordSegment(text: String): Option[Seq[String]] = {
    var arrayBuffer = ArrayBuffer[String]()

    if (text != null && text != "") {
      val tmp = new util.ArrayList[Term]()

      val result: util.List[Term] = ansjSegment(text)
      tmp.addAll(result)

      for (i <- 0 until tmp.size()) {
        val term = tmp.get(i)
        var item = term.getName.trim()
        arrayBuffer += item
      }
    }
    Some(arrayBuffer)

  }

}
