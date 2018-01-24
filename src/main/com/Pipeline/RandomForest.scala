package com.Pipeline

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * @author zhaoming on 2018-01-15 11:49
  **/
object RandomForest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)

    val sqlContext = SQLContext.getOrCreate(sc)

    val parsedRDD =sc.textFile("D:\\code_test\\mllibtest\\data\\mldata1.txt")
      .map(_.split(","))
      .map(eachRow=>{
        val a =eachRow.map(x=>x.toDouble)
        (a(0),a(1),a(2),a(3),a(4))
      })
    val df =sqlContext.createDataFrame(parsedRDD).toDF(
      "f0","f1","f2","f3","label"
    ).cache()
    df.show()

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(df)

   //val df1 =  labelIndexer.transform(df)

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("f0","f1","f2","f3"))
      .setOutputCol("featureVector")

   // val df2 = vectorAssembler.transform(df1)

    val rfClassifier = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("featureVector")
      .setNumTrees(5)
      //.fit(df2)
   // val rfdf = rfClassifier.transform(df2)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)


    //labelConverter.transform(rfdf).show()

    //对训练集进行划分，按比例分成两个部分
    val Array(trainingData,testData) = df.randomSplit(Array(0.6,0.4))

    //create a ml pipeline which is constructed by  for PipelineStage objects
    //then call fit() to perform defined operations on training data.
    val pipeline = new Pipeline().setStages(Array(labelIndexer,vectorAssembler,rfClassifier,labelConverter))
    val model = pipeline.fit(trainingData)

    val predictionResultDF = model.transform(testData)
    predictionResultDF.show()


    //evaluator
    val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("precision")

    val predictionAccuracy = evaluator.evaluate(predictionResultDF)

    println("Testing Error ="+(1.0-predictionAccuracy))


    val randomForestModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    println(randomForestModel.toDebugString)


  }

}











