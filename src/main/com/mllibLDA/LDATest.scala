package com.mllibLDA

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhaoming on 2017-12-08 9:48
  **/
object LDATest {

  def main(args: Array[String]): Unit = {
    //spark

    //=============================读取数据和分词部分将来由hive和UDF替换===================
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

    val resultRDD = new PreUtils().run(data)


    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val tokenDF = resultRDD.toDF("id", "tokens")
    //================================获得DataFrame可操作的数据集========================

    //--向量化
    val minDocFreq = 2 //最小文档频率阈值
    val toTFIDF = true //是否将TF转化为TF-IDF
    val vocabSize = 4000 //词汇表大小

    val vectorizer = new Vectorizer()
      .setMinDocFreq(minDocFreq)
      .setToTFIDF(toTFIDF)
      .setVocabSize(vocabSize)

    val (cvModel, idf) = vectorizer.load(vecModelPath)
    val vectorizedRDD = vectorizer.vectorize(tokenDF, cvModel, idf)

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


    //--输出结果
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

    //保存结果应该重新组成文档-主题分布的DataFrame，存在对应的位置上
    println(docTopics.getClass)
    println("end")

  }


}
