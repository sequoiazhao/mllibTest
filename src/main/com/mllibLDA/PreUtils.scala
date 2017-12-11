package com.mllibLDA

import java.io.Serializable
import java.util

import org.ansj.domain.Term
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.DicAnalysis
import org.apache.commons.lang3.StringUtils
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * @author zhaoming on 2017-12-08 16:47
  **/
class PreUtils extends Logging with Serializable {

   private val baseExpr =
     """[^\w-\s+\u4e00-\u9fa5]""".r //匹配英文字母、数字、中文汉字之外的字符

  private val enExpr = "[A-Za-z]+".r


  def run(data:RDD[(Long,String)]):RDD[(Long,Seq[String])]={
    //数据清洗
    val cleanedRDD = data.map(str => (str._1, baseClean(str._2)))

    //进行分词
    val resultRDD = cleanedRDD.map { line =>
      (line._1, wordSegment(line._2))
    }.filter(_._2.nonEmpty).map(line => (line._1, line._2.get))

    resultRDD
  }

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
