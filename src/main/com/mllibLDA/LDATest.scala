package com.mllibLDA


import java.io.PrintWriter
import java.net.URL

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.apache.spark.sql.hive.HiveContext

import scala.reflect.io.File

/**
  * @author zhaoming on 2017-12-08 9:48
  **/
object LDATest {

  def main(args: Array[String]): Unit = {
    //spark

    //=============================读取数据和分词部分将来由hive和UDF替换===================
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[*]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)

    val sqlcon:SQLContext=new HiveContext(sc)
    //println("ddd"+LDATest.getClass.getResource("") )

    val dataPath = "D:\\code\\mllibtest\\data\\test"
    val vecModelPath = "D:\\code\\mllibtest\\model"
    val ldaModelPath = "D:\\code\\mllibtest\\model\\ldaModel"

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
    tokenDF.show()

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
    val k = 15
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


    val tesp2 = docTopics.map(doc => {
      //过滤掉较小分值的主题
      //val temp = doc._2.filter(_._1 >= 0.1)
      val temp = Array(doc._2.max)

      //应该找到topic word在文章中是否出现过，才能作为标签
      val docResult2 = (doc._1, temp.map(_._2), temp.map(_._1.formatted("%.2f")), topicWords.apply(temp.head._2.toInt).map(_._1))
      docResult2
    })

    //输出结果2

    //保存结果应该重新组成文档-主题分布的DataFrame，存在对应的位置上
    //存的格式是什么？要把主题词和其标签存下来？所属主题也要存下来
    val datax = tesp2.toDF("id", "topic", "score", "word")
    val Joindata = datax.join(tokenDF, Seq("id"))
      // .orderBy("topic")
     // .groupBy("topic","word")
     // .agg(collect_set("id").as("new"))


    Joindata.show()

  //  Joindata.collect().toVector.foreach(ix=>println(ix.getList[Int](0), ix.getList[String](1),ix.getList[Int](2)))



    println("------------------------------end------------------------------------")

    val columnIntersection = udfCompute(Joindata.col("word").cast(ArrayType(StringType)),
      Joindata.col("tokens").cast(ArrayType(StringType)))

    val resultData = Joindata
      .withColumn("intersection", columnIntersection)

    resultData.show()


    //输出结果用的
    val tesp = docTopics.take(7).foreach(doc => {
      val docTopicsArray = doc._2.map(topic => topic._2 + " : " + topic._1)
      println(doc._1 + ":[" + docTopicsArray.mkString(" , ") + "]")
    })

    println("主题-词：")
    println(topicWords.length)
    topicWords.take(15).zipWithIndex.foreach(topic => {
      println("Topic: " + topic._2)
      topic._1.foreach(word => {
        println(word._1 + "\t" + word._2)
      })
    })

  }

  def funCompute(left: Seq[String], right: Seq[String]): Seq[String] = {

    if (left != null && right != null) {
      left.intersect(right)
    } else {
      List() //null
    }

  }

  val udfCompute = udf(funCompute _)


}


//      val docResult = doc._2.map(topic => {
//        //val ss = topic._2 + ":" + topic._1.formatted("%.2f")
//        val ss =( topic._2 , topic._1.formatted("%.2f"))
//        ss
//      })


//val sqlContextResult = SQLContext.getOrCreate(sc)
//import sqlContext.implicits._
//    val DataResult = tesp2.toDF("id", "tokens")

//    val newdata = DataResult.withColumn("newcol",explode(col("tokens")))
//    newdata.show()


//    val tesp = docTopics.take(3).foreach(doc => {
//      val docTopicsArray = doc._2.map(topic => topic._2 + " : " + topic._1)
//      println(doc._1 + ":[" + docTopicsArray.mkString(" , ") + "]")
//    })

//    println("主题-词：")
//    println(topicWords.length)
//    topicWords.take(10).zipWithIndex.foreach(topic => {
//      println("Topic: " + topic._2)
//      topic._1.foreach(word => {
//        println(word._1 + "\t" + word._2)
//      })
//    })
//
//    println(docTopics.getClass)