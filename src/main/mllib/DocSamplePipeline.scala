
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhaoming on 2018-10-19 10:57
  **/
object DocSamplePipeline {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)

    val sqlContext = SQLContext.getOrCreate(sc)


//    // Prepare training documents from a list of (id, text, label) tuples.
//    val training = sqlContext.createDataFrame(Seq(
//      (0L, "a b c d e spark", 1.0),
//      (1L, "b d", 0.0),
//      (2L, "spark f g h", 1.0),
//      (3L, "hadoop mapreduce", 0.0)
//    )).toDF("id", "text", "label")
//
//    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
//    val tokenizer = new Tokenizer()
//      .setInputCol("text")
//      .setOutputCol("words")
//
//    val hashingTF = new HashingTF()
//      .setNumFeatures(1000)
//      .setInputCol(tokenizer.getOutputCol)
//      .setOutputCol("features")
//
//    val lr = new LogisticRegression()
//      .setMaxIter(100)
//      .setRegParam(0.01)
//    val pipeline = new Pipeline()
//      .setStages(Array(tokenizer, hashingTF, lr))
//
//    // Fit the pipeline to training documents.
//    val model = pipeline.fit(training)
//
//    // now we can optionally save the fitted pipeline to disk
//    // model.save("/tmp/spark-logistic-regression-model")
//
//    // we can also save this unfit pipeline to disk
//    //pipeline.save("/tmp/unfit-lr-model")
//
//    // and load it back in during production
//    //val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")
//
//    // Prepare test documents, which are unlabeled (id, text) tuples.
//    val test = sqlContext.createDataFrame(Seq(
//      (4L, "spark i j k"),
//      (5L, "l m"),
//      (6L, "mapreduce spark"),
//      (7L, "apache hadoop"),
//      (8L, "spark f g")
//    )).toDF("id", "text")
//
//    // Make predictions on test documents.
//    model.transform(test)
//      .select("id", "text", "probability", "prediction")
//      .collect()
//      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
//        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
//      }


//    val sentenceData = sqlContext.createDataFrame(Seq(
//      (0, "Hi I heard about Spark"),
//      (3, "I wish Java could use case classes"),
//      (1, "Logistic regression models are neat")
//    )).toDF("label", "sentence")
//
//    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
//    val wordsData = tokenizer.transform(sentenceData)
//    val hashingTF = new HashingTF()
//      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(40)
//    val featurizedData = hashingTF.transform(wordsData)
//    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
//    val idfModel = idf.fit(featurizedData)
//    val rescaledData = idfModel.transform(featurizedData)
//    rescaledData.select("features", "label").take(3).foreach(println)

    import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}

    val sentenceDataFrame = sqlContext.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic,regression,models,are,neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)

    val tokenized = tokenizer.transform(sentenceDataFrame)
    tokenized.select("words", "label").take(3).foreach(println)
    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
    regexTokenized.select("words", "label").take(3).foreach(println)
  }

}
