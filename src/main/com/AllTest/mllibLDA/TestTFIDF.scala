package com.AllTest.mllibLDA


import org.apache.spark.ml.feature.{CountVectorizer, HashingTF, Tokenizer}
import org.apache.spark.mllib.linalg.SparseVector

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

/**
  * @author zhaoming on 2017-12-20 11:48
  **/
object TestTFIDF {


  def main(args: Array[String]): Unit = {

    val ss = Array("hello", "tom", "hello", "jerry", "hello", "hello")

    val tt = ss.map((_, 1)).groupBy(_._1).map(t => (t._1, t._2.size))

    tt.foreach(println)
    val ss2 = tt.toList.sortBy(_._2).reverse
    val tf = ss2 match {
      case x: List[(String, Int)] => x
    }

    tf.foreach(println)


    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val sentenceData = sqlContext.createDataFrame(Seq(
      //      (0, "10 15 50 70 800 120 223 78"),
      (5, "I wish Java could I I I use case classes I wish")
      , (1, "are Logistic regression models neat are are are are are are")
      // ,(2, "你好 中国 人民 大家 可以 真的 中国 中国")
    )).toDF("label", "sentence")
    //sentenceData.show()

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    wordsData.show()
    //wordsData.foreach {println}
    //这一步在数据库中已经完成了


    //生成TF，但是无法回溯
    val hashingTF = new HashingTF().
      setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(4000)
    val featurizedData = hashingTF.transform(wordsData)


    featurizedData.show(false)

    featurizedData.select("words").rdd.map(_.toSeq).foreach(println)


    //生成CountVectorizer，可以回溯到所在的词
    val cvModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setVocabSize(4000)
      .fit(wordsData)

    val tokensLP = cvModel.transform(wordsData)


    tokensLP.show(100, false)

    //定义IDF，逆词频，进行计算
    val idf = new org.apache.spark.ml.feature.IDF().setInputCol("rawFeatures").setOutputCol("features")

    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)


    val idfModel2 = idf.fit(tokensLP)
    val rescaledData2 = idfModel2.transform(tokensLP)
//    rescaledData2.foreach {
//      println
//    }


    val vocabulary = cvModel.vocabulary
    println(vocabulary.mkString(","))

    rescaledData2.foreach(e => {
      val label = e.getAs[Int]("label")
      val str = e.getAs[String]("sentence")
      val words = e.getAs[mutable.WrappedArray[String]]("words").mkString(",")
      val tf = e.getAs[SparseVector]("rawFeatures")
      val originWords = tf.indices.map(i => vocabulary(i)).mkString(",")
      val idf = e.getAs[SparseVector]("features")
      println(
        s"""
//           |$label   $str
//           |$words
           |$tf        $originWords
           |$idf      $originWords
         """.stripMargin)

    val WordsArr = originWords.split(",")
//     println( WordsArr.length)
//      val tt = tf.values.zipWithIndex
//      println(tt.length)
     val tfdata = tf.values.zipWithIndex.map{x=>
        val id = x._2
        (WordsArr(id),x._1)
      }

      val idfdata = idf.values.zipWithIndex.map{x=>
        val id = x._2
        (WordsArr(id),x._1)

      }

      //tfdata.toList.sortBy(_._2).reverse.foreach(println)
     idfdata.toList.sortBy(_._2).reverse.foreach(println)

    })


    println("=========================================")
    // rescaledData.show()
    val test = rescaledData.select("features").collect()

    val test2 = rescaledData2.select("features").collect()


    test2.foreach { x =>
      val ss = x.get(0)
      println(ss)

      ss match {
        case xm: org.apache.spark.mllib.linalg.SparseVector => {
          println(xm.indices.apply(0), xm.values.apply(0))
        }
      }
    }


  }


}
