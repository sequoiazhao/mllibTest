package com.mllibLDA

import breeze.linalg.SparseVector
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhaoming on 2017-12-20 11:48
  **/
object TestTFIDF {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, "10 15 50 70 800 120 223 78"),
      (5, "I wish Java could use case classes I wish"),
      (1, "Logistic regression models are neat")
    )).toDF("label", "sentence")
    //sentenceData.show()

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")

    val wordsData = tokenizer.transform(sentenceData)
    //wordsData.show()
    //wordsData.foreach {println}

    val hashingTF = new HashingTF().
      setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(4000)
    val featurizedData = hashingTF.transform(wordsData)
    // featurizedData.foreach{println}

    val idf = new org.apache.spark.ml.feature.IDF().setInputCol("rawFeatures").setOutputCol("features")

    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)


    println("=========================================")
    // rescaledData.show()
    val test = rescaledData.select("features").collect()

    test.foreach { x =>
      val ss = x.get(0)
      println(ss)

        ss match {
          case xm:org.apache.spark.mllib.linalg.SparseVector =>{
           println(xm.indices.apply(1),xm.values.apply(2))
          }
        }
    }


  }


}
