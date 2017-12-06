package com.mllibtest

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.DicAnalysis
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import java.util

import org.ansj.domain.Term

import scala.collection.mutable.ArrayBuffer


/**
  * @author zhaoming on 2017-12-04 10:45
  **/
object LDATest {

  private val baseExpr =
    """[^\w-\s+\u4e00-\u9fa5]""".r //匹配英文字母、数字、中文汉字之外的字符

  private val enExpr = "[A-Za-z]+".r



  def ansjSegment(text:String):util.List[Term] = {
    val filter = new StopRecognition()
    filter.insertStopNatures("w")
    filter.insertStopWords("的")

    val result =DicAnalysis.parse(text).recognition(filter)
    result.getTerms
  }


  def wordSegment(text:String):Option[Seq[String]]={
    var arrayBuffer = ArrayBuffer[String]()

    if (text != null && text != "") {
      val tmp = new util.ArrayList[Term]()

      val result:util.List[Term] = ansjSegment(text)
      tmp.addAll(result)

      for(i<-0 until tmp.size()){
      val term = tmp.get(i)
        var item = term.getName.trim()
        arrayBuffer+=item
      }
    }
    Some(arrayBuffer)

  }

  def main(args: Array[String]): Unit = {

    //spark

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
    val sc = new SparkContext(conf)


    //读入文件
    val hadoopConf = sc.hadoopConfiguration
    val inPath = "D:/code/mllibtest/data"
    val fs = new Path(inPath).getFileSystem(hadoopConf)
    val len = fs.getContentSummary(new Path(inPath)).getLength / (1024 * 1024)
    val minPart = (len / 32).toInt

    //数据标记
    val data = sc.textFile(inPath, minPart).zipWithIndex().map(_.swap)

    //getText.foreach(println)

    //println(getText.context)

    //数据清洗
    val cleanedRDD = data.map(str => (str._1, baseClean(str._2)))

    //    cleanedRDD.foreach(println)
    //去除符号，增加新的停用词

    val resultRDD = cleanedRDD.map{line=>
      (line._1,wordSegment(line._2))
    }.filter(_._2.nonEmpty).map(line=>(line._1,line._2.get))

    resultRDD.foreach(println)


    //向量化
    val minDocFreq = 2
    val toTFIDF = true
    val vocabSize =5000

    val vectorizer = new Vectorizer()





//    var resultRDD = cleanedRDD.map { line =>
//      (line._1, DicAnalysis.parse(line._2).recognition(filter).toStringWithOutNature().split(",").toSeq())
//    }.filter(_._2.nonEmpty).map(line => (line._1, line._2)

    //    resultRDD.count()
    //    resultRDD.foreach(println)


    //println(DicLibrary.DEFAULT)
    //分词，转化RDD
    //    val filter = new StopRecognition()
    //    filter.insertStopNatures("w")
    //    filter.insertStopWords("的")
    //
    //
    //    val testsentence = NlpAnalysis.parse("在ltvrsV1.0项目开发过程中，根据系统端测试需求，基于协同过滤等算法提高直播和点播节目匹配结果的准确度；进一步分析直播节目的全量和增量更新方法；\n\t引入新的时间、年代、描述性说明等标签，增加匹配的计算维度；统计现有媒资标签分布情况，优化标签的数量和准确性。")
    //      .recognition(filter)
    //      .toStringWithOutNature()
    //
    //    if (testsentence.length>0) {
    //    //  testsentence.split(",").foreach(println)
    //    }


  }

  /**
    * 针对单行记录
    * 基础清理，包括:繁转简体、全角转半角、去除不可见字符、数值替换、去英文
    * 输入数据
    * @return 经过基础清洗的数据
    */


  def baseClean(line: String): String = {
    var result = line.trim
    val numToChar = "数"
    val f2j = false


    //去除不可见字符
    result = baseExpr.replaceAllIn(result, "")
    result = StringUtils.trimToEmpty(result)


    //去除英文字符
    result = enExpr.replaceAllIn(result, "")

    result
  }


}
