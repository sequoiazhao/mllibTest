package com.mllibLDA

import java.util

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.DicAnalysis
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.ansj.domain.Term
import org.apache.spark.mllib.clustering.LDAModel
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ArrayBuffer


/**
  * @author zhaoming on 2017-12-04 10:45
  **/
object LDATrain {


  def main(args: Array[String]): Unit = {

    //spark

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)

    //读入文件
    val hadoopConf = sc.hadoopConfiguration
    val inPath = "D:/code/mllibtest/data/train"
    val fs = new Path(inPath).getFileSystem(hadoopConf)
    val len = fs.getContentSummary(new Path(inPath)).getLength / (1024 * 1024)
    val minPart = (len / 32).toInt

    //数据标记
    val data = sc.textFile(inPath, minPart).zipWithIndex().map(_.swap)

    //getText.foreach(println)

    //println(getText.context)

    //数据清洗
    val cleanedRDD = data.map(str => (str._1, baseClean(str._2)))

    //cleanedRDD.foreach(println)
    //去除符号，增加新的停用词

    val resultRDD = cleanedRDD.map { line =>
      (line._1, wordSegment(line._2))
    }.filter(_._2.nonEmpty).map(line => (line._1, line._2.get))

    // resultRDD.foreach(println)


    //向量化
    val minDocFreq = 2 //最小文档频率阈值
    val toTFIDF = true //是否将TF转化为TF-IDF
    val vocabSize = 4000 //词汇表大小

    val vectorizer = new Vectorizer()
      .setMinDocFreq(minDocFreq)
      .setToTFIDF(toTFIDF)
      .setVocabSize(vocabSize)


    val vecModelPath = "D:/code/mllibtest/model"
    val ldaModelPath = "D:/code/mllibtest/model/ldaModel"


    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val tokenDF = resultRDD.toDF("id", "tokens")

    val (vectorizedRDD, cvModle, idf) = vectorizer.vectorize(tokenDF)
    vectorizer.save(vecModelPath, cvModle, idf)
    println("end")

    println("===========================")
    vectorizedRDD.foreach(println)

    println("===========================")
    val trainRDD = vectorizedRDD.map(line => (line.label.toLong, line.features))
    trainRDD.foreach(println)

    //LDA 训练

    val k = 10 //主题的个数
    val analysisType = "em" //参数估计
    val maxIterations = 20 //迭代次数

    val ldaUtils = new LDAUtils()
      .setK(k)
      .setAlgorithm(analysisType)
      .setMaxIterations(maxIterations)

    val ldaModel: LDAModel = ldaUtils.train(trainRDD)
    ldaUtils.save(sc, ldaModelPath, ldaModel)

    sc.stop()

  }


  private val baseExpr =
    """[^\w-\s+\u4e00-\u9fa5]""".r //匹配英文字母、数字、中文汉字之外的字符

  private val enExpr = "[A-Za-z]+".r


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


  //return 经过基础清洗的数据
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


}


