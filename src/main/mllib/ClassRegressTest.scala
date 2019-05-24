

import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * @author zhaoming on 2018-11-01 17:08
  **/
object ClassRegressTest {

  def main(args: Array[String]): Unit = {
    //environment
    val conf = new SparkConf()
      .setAppName("LDATest")
      .setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val training = sqlContext.read.format("libsvm").load("d:\\data\\sample_libsvm_data.txt")

    val lr = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(0.4)
      .setElasticNetParam(0.2)

    val lrModel = lr.fit(training)

    val result = lrModel.transform(training)
    result.select(
      "label"
      , "rawPrediction"
      , "probability"
      , "prediction"
    ) show (false)

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    val trainingSummary = lrModel.summary

    val objectiveHistory = trainingSummary.objectiveHistory
    objectiveHistory.foreach(println)

    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]
    val roc = binarySummary.roc
    roc.show()
    println(binarySummary.areaUnderROC)


    val fMeasure = binarySummary.fMeasureByThreshold

    val maxFMeasure =fMeasure.select(max("F-Measure")).head().getDouble(0)
    val bestThreshold = fMeasure.where(col("F-Measure").=== (lit(maxFMeasure)))
      .select("threshold").head().getDouble(0)
    print(bestThreshold)
    lrModel.setThreshold(bestThreshold)


    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(training)

    labelIndexer.transform(training).select("label","indexedLabel")show(false)
    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous
      .fit(training)


  }

}
