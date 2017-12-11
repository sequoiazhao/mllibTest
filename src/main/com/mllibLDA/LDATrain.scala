package com.mllibLDA

import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.clustering.LDAModel
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


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

    val resultRDD = new PreUtils().run(data)

    // resultRDD.foreach(println)


    val vecModelPath = "D:/code/mllibtest/model"
    val ldaModelPath = "D:/code/mllibtest/model/ldaModel"


    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val tokenDF = resultRDD.toDF("id", "tokens")
    println("输入数据的长度"+tokenDF.count())
//=========================================================

    //向量化
    val minDocFreq = 2 //最小文档频率阈值
    val toTFIDF = true //是否将TF转化为TF-IDF
    val vocabSize = 4000 //词汇表大小

    val vectorizer = new Vectorizer()
      .setMinDocFreq(minDocFreq)
      .setToTFIDF(toTFIDF)
      .setVocabSize(vocabSize)

    val (vectorizedRDD, cvModle, idf) = vectorizer.vectorize(tokenDF)
    vectorizer.save(vecModelPath, cvModle, idf)
    println("end")

    println("===========================")
    vectorizedRDD.foreach(println)

    println("===========================")
    val trainRDD = vectorizedRDD.map(line => (line.label.toLong, line.features))
    trainRDD.foreach(println)

    //LDA 训练

    val k = 15 //主题的个数
    val analysisType = "em" //参数估计
    val maxIterations = 30 //迭代次数

    val ldaUtils = new LDAUtils()
      .setK(k)
      .setAlgorithm(analysisType)
      .setMaxIterations(maxIterations)

    val ldaModel: LDAModel = ldaUtils.train(trainRDD)
    ldaUtils.save(sc, ldaModelPath, ldaModel)

    sc.stop()

  }


}


