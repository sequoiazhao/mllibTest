package com.MLToolsTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.ml.tree.InternalNode
import org.apache.spark.mllib.tree.model.{DecisionTreeModel => OldDecisionTreeModel}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhaoming on 2018-03-23 10:42
  **/
object TestPipleGBT {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.FATAL)
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

//    val data = sqlContext.read.format("libsvm").load("D:\\data\\sample_libsvm_data.txt")
//    data.show()

    //    val featureIndexer = new VectorIndexer()
    //      .setInputCol("features")
    //      .setOutputCol("indexedFeatures")
    //      .setMaxCategories(4)
    //      .fit(data)
    val parsedRDD = sc.textFile("D:\\code_test\\mllibtest\\data\\mldata1.txt")
      .map(_.split(","))
      .map(eachRow => {
        val a = eachRow.map(x => x.toDouble)
        (a(0), a(1), a(2), a(3), a(4))
      })
    val data = sqlContext.createDataFrame(parsedRDD).toDF(
      "f0", "f1", "f2", "f3", "label"
    ).cache()
    data.show()


    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)

    //val df1 =  labelIndexer.transform(df)

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("f0", "f1", "f2", "f3"))
      .setOutputCol("featureVector")

    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    val gbt = new GBTRegressor()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("featureVector")
      .setMaxIter(50)

    val pipeline = new Pipeline()
      .setStages(Array(gbt))

    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("prediction", "label", "features").show(50)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

    val gbtModel = model.stages(0).asInstanceOf[GBTRegressionModel]
    println(s"Learned regression GBT model:\n ${gbtModel.toDebugString}")

    //  println(gbtModel.trees(0).rootNode.getClass)
    gbtModel

    //    model.write.overwrite().save("D:\\data\\model.txt")

    //    val fileOut:FileOutputStream  = new FileOutputStream("D:\\data\\model.txt")
    //    val out:ObjectOutputStream  = new ObjectOutputStream(fileOut)
    //    out.writeObject(gbtModel)

//    gbtModel.trees(0).rootNode match {
//      case x: InternalNode =>
//        //println(x.rightChild)
//
//        val ss = Class.forName("org.apache.spark.ml.tree.InternalNode")
//        val tt = ss.getDeclaredField("impurityStats")
//        tt.setAccessible(true)
//        val m = tt.get(x)
//      //  tt.setInt(x,1)
//
//        val ssx = Class.forName("org.apache.spark.ml.tree.InternalNode")
//        val method = ssx.getDeclaredMethod("toOld", classOf[Int])
//        val sst = method.invoke(x, new Integer(1))
//        //    println(sst.getClass)
//        val re = sst.asInstanceOf[org.apache.spark.mllib.tree.model.Node]
//        println(re.predict)
//
//
//      //            println(x.split.featureIndex)
//      //            println(x.leftChild)
//      //            //
//      //            val xs = x.leftChild
//      //            xs match {
//      //              case xx: InternalNode => println(xx.leftChild)
//      //            }
//    }
//    //println(x.rightChild)
//    val trees = gbtModel.trees
//
//    val treeWeights = gbtModel.treeWeights
//
//    val header = toString + "\n"
//    header + trees.zip(treeWeights).zipWithIndex.map { case ((tree, weight), treeIndex) =>
//      val s1 = s"  Tree $treeIndex (weight $weight):\n"
//      val ss = Class.forName("org.apache.spark.ml.tree.Node")
//      val method = ss.getDeclaredMethod("subtreeToString", classOf[Int])
//      val sst = method.invoke(tree.rootNode, new Integer(4))
//      //        tree.rootNode.subtreeToString(4)
//      //        tree.rootNode
//      val sr = s1 + sst
//      println(sr)
//      sr
//
//    }.fold("")(_ + _)
//    println("hello")


    //    val ss =Class.forName("org.apache.spark.ml.tree.InternalNode")
    //    val method = ss.getDeclaredMethod("toOld",classOf[Int])
    //    val sst =method.invoke(ss.newInstance(), new Integer(1))
    //    println(sst.getClass)


    //    import scala.reflect.runtime.{universe => ru}
    //
    //    val classMirror = ru.runtimeMirror(getClass.getClassLoader)
    //    val classTest = classMirror.reflect(new InternalNode)
    //
    //    val methods = ru.typeOf[InternalNode].typeSymbol.asClass
    //    val method = classMirror.reflectClass(methods)
    //
    //    val ctor = ru.typeOf[InternalNode].declaration(ru).asMethod


    //  println(header)
    //        gbtModel.trees(0).rootNode match {
    //          case x:org.apache.spark.ml.tree.Convert=>
    //        }

    //    implicit  def ML2Mllib(d:org.apache.spark.ml.tree.Convert) =gbtModel.trees(0).rootNode
    //    val ss: org.apache.spark.ml.tree.Convert=gbtModel.trees(0).rootNode
    //    val sst =ML2Mllib(ss)


  }


  //    gbtModel.trees(0).rootNode match {
  //      case x:org.apache.spark.ml.treex.InternalNode=>x.impurityStats
  //    }


  //    val sss = new GradientBoostedTreesModel(Algo.Regression, gbtModel.trees.map { s =>
  //      val rootNodeOld = s.rootNode match {
  //        case x: org.apache.spark.ml.treex.InternalNode => //getOld(x,1)
  //
  //
  //      }
  //
  //      new DecisionTreeModel(rootNodeOld, Algo.Regression)
  //    }
  //      , gbtModel.treeWeights)
  //
  //    println(sss.trees)


  //    val header = toString + "\n"
  //    header + trees.zip(weights).zipWithIndex.map { case ((tree, weight), treeIndex) =>
  //
  //      s"  Tree $treeIndex (weight $weight):\n"+ tree.rootNode.prediction
  //    }.fold("")(_ + _)


  //  def getOld(rootNode: InternalNode,id:Int): org.apache.spark.mllib.tree.model.Node = {
  //
  //
  //
  //      new org.apache.spark.mllib.tree.model.Node(id, new org.apache.spark.mllib.tree.model.Predict(rootNode.prediction
  //      , prob = rootNode.impurityStats.prob(rootNode.prediction)), rootNode.impurity,
  //      isLeaf = false, Some(rootNode.split.toOld), Some(rootNode.leftChild.toOld(org.apache.spark.mllib.tree.model.Node.leftChildIndex(id))),
  //      Some(rootNode.rightChild.toOld(org.apache.spark.mllib.tree.model.Node.rightChildIndex(id))),
  //
  //      Some(new  org.apache.spark.mllib.tree.model.InformationGainStats(rootNode.gain, rootNode.impurity, rootNode.leftChild.impurity, rootNode.rightChild.impurity,
  //        new org.apache.spark.mllib.tree.model.Predict(rootNode.leftChild.prediction, prob = 0.0),
  //        new org.apache.spark.mllib.tree.model.Predict(rootNode.rightChild.prediction, prob = 0.0))))
  //
  //  }

}


//val sss = new GradientBoostedTreesModel(Algo.Regression,gbtModel.trees.map{s=>
//
//new DecisionTreeModel(s.rootNode.asInstanceOf[org.apache.spark.mllib.tree.model.Node],Algo.Regression)}
//,gbtModel.treeWeights)
//
//println(sss.trees)
