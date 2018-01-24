package com.Pipeline

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.clustering.OnlineLDAOptimizer
import org.apache.spark.sql.{DataFrame, Row}

/**
  * @author zhaoming on 2018-01-17 17:02
  **/
class LDAml() {

//  def lda(dataset: DataFrame, sc: SparkContext, inputCol: String,
//          numbTopic: Int, MaxIterations: Int,
//          vocabSize: Int) = {
//
//    val (documents, vocabArray, model) = preprocess(dataset, inputCol, sc, vocabSize)
//    val corpus = documents.cache() // use cache
//    val corpusSize = corpus.count()
//    /**
//      * Configure and run LDA
//      */
//    val mbf = {
//      // add (1.0 / actualCorpusSize) to MiniBatchFraction be more robust on tiny datasets.
//      2.0 / MaxIterations + 1.0 / corpusSize
//    }
//    // running lda
//    val lda = new LDA()
//      .setK(numbTopic)
//      .setMaxIterations(MaxIterations)
//      .setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(math.min(1.0, mbf))) //add optimizer
//      .setDocConcentration(-1) // use default symmetric document-topic prior
//      .setTopicConcentration(-1) // use default symmetric topic-word prior
//
//    /**
//      * Print results.
//      */
//    val startTime = System.nanoTime()
//    val ldaModel = lda.run(corpus)
//    val elapsed = (System.nanoTime() - startTime) / 1e9
//
//    ldaModel.save(sc, sc.getConf.get("spark.client.ldamodelPath"))
//
//    /** **********************************************************************
//      * Print results. for Zeppelin
//      * ***********************************************************************/
//    // Print training time
//    println(s"Finished training LDA model.  Summary:")
//    println(s"Training time (sec)\t$elapsed")
//    println(s"==========")
//
//    // Print the topics, showing the top-weighted terms for each topic.
//    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 5)
//    val topics = topicIndices.map {
//      case (terms, termWeights) =>
//        terms.map(vocabArray(_)).zip(termWeights)
//    }
//    println(s"$numbTopic topics:")
//    topics.zipWithIndex.foreach {
//      case (topic, i) =>
//        println(s"TOPIC $i")
//        topic.foreach { case (term, weight) => println(s"$term\t$weight") }
//        println(s"==========")
//    }
//  }
//
//  def preprocess(dataset: DataFrame, inputCol: String, sc: SparkContext, vocabSize: Int): (RDD[(Long, Vector)], Array[String], PipelineModel) = {
//    val stopWordText1 = sc.textFile(sc.getConf.get("spark.client.stopWordText")).collect().flatMap(_.stripMargin.split("\\s+"))
//    val stopWordText2 = sc.textFile(sc.getConf.get("spark.client.stopWordText2")).collect().flatMap(_.stripMargin.split("\\s+"))
//    val data = dataset.na.drop()
//    // ----------------Pipeline stages---------------------------------------------
//    // - tokenizer-->stopWordsRemover1-->stemmer-->stopWordRemover2-->AccentRemover
//    // 简单分词->删除无用词汇->词根->删除无用词汇->删除重音符号
//    // ----------------------------------------------------------------------------
//    val tokenizer = new RegexTokenizer()
//      .setInputCol(inputCol)
//      .setPattern("[a-z0-9éèêâîûùäüïëô]+")
//      .setGaps(false)
//      .setOutputCol("rawTokens")
//
//    val stopWordsRemover1 = new StopWordsRemover()
//      .setInputCol("rawTokens")
//      .setOutputCol("tokens")
//    stopWordsRemover1.setStopWords(stopWordsRemover1.getStopWords ++ stopWordText1)
//
//    val stemmer = new Stemmer()
//      .setInputCol("tokens")
//      .setOutputCol("stemmed")
//      .setLanguage("French")
//
//    val stopWordsRemover2 = new StopWordsRemover()
//      .setInputCol("stemmed")
//      .setOutputCol("tokens2")
//    stopWordsRemover2.setStopWords(stopWordsRemover2.getStopWords ++ stopWordText2)
//
//    val accentRemover = new AccentRemover()
//      .setInputCol("tokens2")
//      .setOutputCol("mot")
//
//    val countVectorizer = new CountVectorizer()
//      .setVocabSize(vocabSize)
//      .setInputCol("mot")
//      .setOutputCol("features")
//    //------------------------------------------------------
//    //stage 0,1,2,3,4,5
//    val pipeline = new Pipeline().setStages(Array(
//      tokenizer, //stage 0
//      stopWordsRemover1, //1
//      stemmer, //2
//      stopWordsRemover2, //3
//      accentRemover, //4
//      countVectorizer //stage 5
//    ))
//
//    // creates the PipeLineModel to use for the dataset transformation
//    val model = pipeline.fit(data)
//    // countVectorizer stage ==> 5
//    val vocabArray = model.stages(5).asInstanceOf[CountVectorizerModel].vocabulary
//    sc.parallelize(vocabArray).saveAsTextFile(sc.getConf.get("spark.client.vocab"))
//    val documents = model.transform(data)
//      .select("features")
//      .rdd
//      .map { case Row(features: Vector) => features }
//      .zipWithIndex()
//      .map(_.swap)
//
//    (documents, vocabArray, model)
  }
