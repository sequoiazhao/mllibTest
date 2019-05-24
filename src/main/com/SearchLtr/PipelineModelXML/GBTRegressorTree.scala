package com.SearchLtr.PipelineModelXML

import com.MLToolsTest.Pipeline.Functions
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, GBTRegressionModel, GBTRegressor}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhaoming on 2018-06-26 16:02
  **/
object GBTRegressorTree {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)

    val sqlContext = SQLContext.getOrCreate(sc)

    val parsedRDD = sc.textFile("D:\\code_test\\mllibtest\\data\\mldata1.txt")
      .map(_.split(","))
      .map(eachRow => {
        val a = eachRow.map(x => x.toDouble)
        (a(0), a(1), a(2), a(3), a(4))
      })
    val df = sqlContext.createDataFrame(parsedRDD).toDF(
      "f0", "f1", "f2", "f3", "label"
    ).cache()
    df.show()

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(df)

    //val df1 =  labelIndexer.transform(df)

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("f0", "f1", "f2", "f3"))
      .setOutputCol("featureVector")

    // val df2 = vectorAssembler.transform(df1)

    // val rfClassifier = new RandomForestClassifier()
    val rfClassifier = new GBTRegressor()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("featureVector")
      .setMaxIter(100)
      .setMaxDepth(5)
    //.setNumTrees(5)
    //.fit(df2)
    // val rfdf = rfClassifier.transform(df2)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)


    //labelConverter.transform(rfdf).show()

    //对训练集进行划分，按比例分成两个部分
    val Array(trainingData, testData) = df.randomSplit(Array(0.6, 0.4))

    //create a ml pipeline which is constructed by  for PipelineStage objects
    //then call fit() to perform defined operations on training data.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, vectorAssembler, rfClassifier, labelConverter))
    val model = pipeline.fit(trainingData)

    val predictionResultDF = model.transform(testData)
    predictionResultDF.show()


    //evaluator
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
    //  .setMetricName("precision")

    val predictionAccuracy = evaluator.evaluate(predictionResultDF)

    println("Testing Error =" + (1.0 - predictionAccuracy))

    val gBTRegressorModel = model.stages(2).asInstanceOf[GBTRegressionModel]
    println(gBTRegressorModel.toDebugString)

    tempTestTree(gBTRegressorModel)


  }

  def tempTestTree(model: GBTRegressionModel): Unit = {
    val trees = model.trees
    val weights = model.treeWeights
    var output = "## RandomForest\n"
    output += "## No. of trees = " + model.numTrees + "\n"
    output += "\n"
    output += "<ensemble>" + "\n"
    output += trees.zip(weights).zipWithIndex.foldLeft("") { case (result, ((tree, weight), index)) =>
      //Change type to DecisionTreeClassificationModel
      val dsTree = tree.asInstanceOf[DecisionTreeRegressionModel]
      result + "\t<tree id=\"" + (index + 1) + "\" weight=\"" + weight + "\">" + "\n" +
      printTree(dsTree) + "\t</tree>" + "\n"

    }
    output += "</ensemble>\n"

    println(output)
  }

  def printTree(tree: DecisionTreeRegressionModel, indent: String = "\t\t"): String = {
    indent + "<split>" + "\n" +
      getClassificationNodeString(tree.rootNode, indent + "\t") +
      indent + "</split>" + "\n"
  }

  def getClassificationNodeString(node: org.apache.spark.ml.tree.Node, indent: String = "\t\t"): String = {

    //Judge the type of node_type
    val node_type = node match {
      case internal: org.apache.spark.ml.tree.InternalNode => "internal"
      case other => "leaf"
    }

    //Get feature
    val feature_index: Option[Int] = node_type match {
      case "internal" => Some(node.asInstanceOf[org.apache.spark.ml.tree.InternalNode].split.featureIndex)
      case "leaf" => None
    }

    //Get split_type
    val split_type: Option[String] = node_type match {
      case "internal" => Some(Functions.get_split_type(node.asInstanceOf[org.apache.spark.ml.tree.InternalNode].split))
      case "leaf" => None
    }

    //Get threshold
    val node_threshold: Option[Double] = split_type match {
      case Some("continuous") => Some(node.asInstanceOf[org.apache.spark.ml.tree.InternalNode]
        .split.asInstanceOf[org.apache.spark.ml.tree.ContinuousSplit].threshold)
      case Some("categorical") => None
      case other => None
    }

    //Use node_type to decide indent
    if (node_type == "leaf") {
      indent + "<output> " + node.prediction.toString.mkString + " </output>" + "\n"
    } else {
      indent + "<feature>" + feature_index.mkString + "</feature>" + "\n" +
        indent + "<threshold> " + node_threshold.mkString + " </threshold>" + "\n" +
        indent + "<split pos=\"left\">" + "\n" +
        getClassificationNodeString(
          node.asInstanceOf[org.apache.spark.ml.tree.InternalNode].leftChild, indent + "\t") +
        indent + "</split>" + "\n" +
        indent + "<split pos=\"right\">" + "\n" +
        getClassificationNodeString(
          node.asInstanceOf[org.apache.spark.ml.tree.InternalNode].rightChild, indent + "\t") +
        indent + "</split>" + "\n"
    }

  }


}
