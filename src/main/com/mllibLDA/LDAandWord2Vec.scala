package com.mllibLDA

import org.apache.spark.ml.feature.Word2Vec

/**
  * @author zhaoming on 2018-01-09 13:38
  **/
object LDAandWord2Vec extends splitWord {
  val inPath = "D:/code_test/mllibtest/data/train"

  val vecModelPath = "D:/code_test/mllibtest/model"
  val ldaModelPath = "D:/code_test/mllibtest/model/ldaModel"

  def main(args: Array[String]): Unit = {

    val splitWords = getSplitWordsDF(inPath, 15)
    splitWords.show(false)

    //1、调用word2Vec
   val word2Vec = new Word2Vec()
      .setInputCol("tokens")
       .setOutputCol("result")
      .setVectorSize(10)
      .setMinCount(1)

    val model = word2Vec.fit(splitWords)

    val result = model.transform(splitWords)

    result.select("result").take(10).foreach(println)


    val syn = model.findSynonyms("喜剧",5)
    syn.show()

    //需要测试一下syn在大数据集上的效果，看能否找到相似词

    model.getVectors.show(false)

   // println(model.getVectors.getClass)




  }


}
