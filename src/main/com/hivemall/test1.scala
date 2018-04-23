package com.hivemall

import com.mllibLDA.{PreUtils, Vectorizer}
import com.sun.org.apache.bcel.internal.generic.ArrayType
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.HmLabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.HivemallOps._
import org.apache.spark.sql.hive.HivemallUtils._
import org.apache.spark.sql.functions._



/**
  * @author zhaoming on 2018-01-24 15:58
  **/
object test1 {

  val inPath = "D:/code_test/mllibtest/data/train"
  val vecModelPath = "D:/code_test/mllibtest/model"
  val ldaModelPath = "D:/code_test/mllibtest/model/ldaModel"


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.FATAL)
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)

    val sqlhiveContext = new HiveContext(sc)

    //    //读入文件
    //    val hadoopConf = sc.hadoopConfiguration
    //
    //    val fs = new Path(inPath).getFileSystem(hadoopConf)
    //    val len = fs.getContentSummary(new Path(inPath)).getLength / (1024 * 1024)
    //    val minPart = (len / 32).toInt
    //
    //    //数据标记
    //    val data = sc.textFile(inPath, minPart).zipWithIndex().map(_.swap)
    //
    //    //分词在这里
    //    val resultRDD = new PreUtils().run(data)
    //
    //    val sqlContext = SQLContext.getOrCreate(sc)
    //    import sqlContext.implicits._
    //    val tokenDFx = resultRDD.toDF("idx", "tokens")
    //    val tokenDF = tokenDFx.withColumn("id", monotonically_increasing_id()).limit(10)
    //      .drop("idx")
    //
    //
    //    //=========================================================
    //
    //    //向量化
    //    val minDocFreq = 2 //最小文档频率阈值
    //    val toTFIDF = true //是否将TF转化为TF-IDF
    //    val vocabSize = 4000 //词汇表大小
    //
    //    val vectorizer = new Vectorizer()
    //      .setMinDocFreq(minDocFreq)
    //      .setToTFIDF(toTFIDF)
    //      .setVocabSize(vocabSize)
    //
    //    val (vectorizedRDD, cvModle, idf) = vectorizer.vectorize(tokenDF)
    //
    //
    //    println("===========================")
    //    val trainRDD = vectorizedRDD.map(line => (line.label.toLong, line.features))


    // sqlhiveContext.sql("CREATE TEMPORARY FUNCTION hivemall_version AS 'hivemall.HivemallVersionUDF'").show()
    //org/apache/spark/sql/hive/HivemallOps

    import org.apache.spark.mllib.util.MLUtils

    // val trainRDD = MLUtils.loadLibSVMFile(sc, "d:/code_test/mllibtest/data/a9a.t")
    //trainRDD
    //val sqlContext = SQLContext.getOrCreate(sc)

    val tf = sqlhiveContext.read.format("libsvm").load("d:/code_test/mllibtest/data/a9a.t")
    //    import sqlhiveContext.implicits._
    //    val tf = trainRDD.toDF()
    //    tf.show()

    tf.show()
    //    val (max, min) = tf.select(max($"label"), min($"label")).collect.map {
    //      case Row(max: Double, min: Double) => (max, min)
    //    }

    val x1 = tf.select(max("label"), min("label")).collect.map {
      case Row(max: Double, min: Double) => (max, min)
    }

    println(x1.apply(0)._1, x1.apply(0)._2)
    //    trainRDD.toDF().select(
    //      // `label` must be [0.0, 1.0]
    //      rescale($"label", lit("min"), lit("max")).as("label"),
    //      $"features"
    //    )

    val trainDf = tf.select(
      rescale("label", lit(x1.apply(0)._2), lit(x1.apply(0)._1)).as("label"),
      "features"
    )

    //    val test =  tf.select(rowid(),
    //      rescale("label", lit(x1.apply(0)._2), lit(x1.apply(0)._1)).as("label"),
    //      "features"
    //    )

    trainDf.printSchema()
    // test.show()

    val testDf = sqlhiveContext.read.format("libsvm").load("d:/code_test/mllibtest/data/a9a.t")
      .select(rowid(), rescale("label", lit(x1.apply(0)._2), lit(x1.apply(0)._1)).as("label"), "features")
      .select("rowid", "label".as("target"), "feature", "weight".as("value"))
      .cache

    testDf.printSchema()

//    val modelDf = trainDf
//      .train_logregr(append_bias($"features"), $"label")
//      .groupBy("feature").avg("weight")
//      .toDF("feature", "weight")
//      .cache





  }


}
