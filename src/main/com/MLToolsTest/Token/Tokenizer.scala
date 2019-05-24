package com.MLToolsTest.Token

import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * @author zhaoming on 2018-01-15 14:30
  **/
object Tokenizer {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)


    val sentenceDataFrame = sqlContext.createDataFrame(Seq(
      (0, "Hi I heard about Spark, i"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic,regression,models,are,neat"),
      (3, "中华人民共和国解放军电影声音，语文")
    )).toDF("id", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")

    tokenizer.transform(sentenceDataFrame).show()

    //    val vs :Vector = Vectors.sparse(4,Array(0,1,2,3),Array(9,5,2,7))
    //    println(vs(1))

    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)

    val countTokens = udf { (words: Seq[String]) => words.length }

    val tokenized = tokenizer.transform(sentenceDataFrame)
    tokenized.select("sentence", "words")
      .withColumn("tokens", countTokens(col("words"))).show(false)

    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
    regexTokenized.select("sentence", "words")
      .withColumn("tokens", countTokens(col("words"))).show(false)

  }

}
