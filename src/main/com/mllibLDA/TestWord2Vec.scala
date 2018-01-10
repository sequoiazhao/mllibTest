package com.mllibLDA
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{Tokenizer, Word2Vec}
import org.apache.spark.sql.SQLContext
/**
  * @author zhaoming on 2018-01-09 18:01
  **/
object TestWord2Vec {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)

    val sqlContext = SQLContext.getOrCreate(sc)

    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = sqlContext.createDataFrame(Seq(
      "你好 中国 人民 大家 可以 真的 中国 中国".split(" ")
//      "推荐 系统 系统 推荐 也许 大家 都是".split(" "),
//      "什么 什么 东西 东西 我们 你模拟 大家".split(" ")
    ).map(Tuple1.apply)).toDF("text")
    documentDF.show()


    val sentenceData = sqlContext.createDataFrame(Seq(
      //      (0, "10 15 50 70 800 120 223 78"),
//      (5, "I wish Java could use case classes I wish")
//      ,(1, "are Logistic regression models neat are are are are are are")
       (2, "你好 很好")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("text")
    val wordsData = tokenizer.transform(sentenceData)
    wordsData.show()

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(1)

    val model = word2Vec.fit(wordsData)
    val result = model.transform(wordsData)

    //val syn = model.findSynonyms("中国",5)

    //syn.show()

    model.getVectors.foreach(println)//文章中的每个词都有一个三维向量表示

    //result.show()
    result.select("result").take(10).foreach(println) //获得所有词向量纵向平均值，能表示文章什么呢？
    //怎么聚类的，结果怎么用？
  }

}
