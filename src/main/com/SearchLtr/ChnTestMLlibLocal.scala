package com.SearchLtr


import com.jayway.jsonpath.JsonPath
import net.minidev.json.JSONArray
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, GradientBoostedTreesModel, Node}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{col, floor, lit, log}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.json4s.jackson.JsonMethods.parse

import scalaj.http.{Http, HttpOptions}

/**
  * @author zhaoming on 2018-04-17 14:26
  **/
object ChnTestMLlibLocal {
  val numIterations = 100

  val maxDepth = 3

  val featureStart = 4

  val featureEnd = 10

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val MyContext = new HiveContext(sc)

    val mdd = sc.textFile("D:\\dataframeData\\chnlocal01")

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val datardd = mdd.map { x =>
      val s1 = x.replace("[", "").replace("]", "").split(",")
      (s1(0), s1(1), s1(2), s1(3), s1(4), s1(5), s1(6), s1(7), s1(8), s1(9), s1(10))
    }
    val SampleDataOriginal = datardd.toDF("score", "keyid", "rank", "mediaid", "lengthweight", "logplaytimes", "logsearchtimes", "new", "fee", "categoryweight", "srcsearchkey")
      .withColumn("score2", floor(log(col("score").+(lit(1.7183)))))
      .drop("score").withColumnRenamed("score2", "score")

    val tdd = sc.textFile("D:\\dataframeData\\chnlocal02")

    val relData = tdd.map { x =>
      val s1 = x.replace("[", "").replace("]", "").replace(", ", ":").split(",")
      (s1(0), s1(1))
    }.toDF("srcsearchkey", "mediaid")

    val data = relData.rdd.take(relData.count.toInt).map { mex =>
      // println(mex.get(1).toString.replace("WrappedArray(","[\"").replace(")","\"]").replace(", ","\",\""))
      //      println(mex.get(0).toString)
      val searchx =
        """{
            "query": {
              "bool": {
                "filter": [
                  {
                    "terms": {
                      "_id": """ + mex.get(1).toString.replace("WrappedArray(", "[\"").replace(")", "\"]").replace(":", "\",\"") +
          """
                    }
                  },
                  {
                    "sltr": {
                      "_name": "logged_featureset",
                      "featureset": "v40_vod_chinese_similarity",
                      "params": {
                        "keywords": """ + "\"" + mex.get(0).toString + "\"" +
          """
                      }
                    }
                  }
                ]
              }
            },
            "ext": {
              "ltr_log": {
                "log_specs": {
                  "name": "log_entry1",
                  "named_query": "logged_featureset"
                }
              }
            },
           "_source": {
 	             "includes": "log_entry1"
            }
          }""".stripMargin

      //println(searchx)
      //val result = Http("http://10.18.210.224:9600/unionsearch_vod/_search")
      val result = Http("http://10.18.210.224:9214/unionsearch_vod_online/_search?size=100")

        .postData(searchx)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(100000)).asString


      println(result)


      val json = JsonPath.parse(result.body)
      val resultId: JSONArray = json.read("$.hits.hits[*]._id")
      val resultLog: JSONArray = json.read("$.hits.hits[*].fields._ltrlog[*].log_entry1")


      val idArray = resultId.toArray
      println(idArray.length)
      val logArray = resultLog.toArray

      val logParse = logArray.map { x =>
        val sx = x.toString match {
          case ax: String => parse(ax)
        }
        sx.values
      }

      val LogValueList = logParse.map { x =>
        val result = x match {
          case sx: List[Map[String, String]] => val sxResult = sx.map {
            ssx =>
              if (ssx.get("value").isEmpty) {
                0.0
              } else {
                ssx("value")
              }
          }
            sxResult
        }
        result
      }

      val LogValueArray = LogValueList.flatten
      //st5.foreach(println)

      idArray.zipWithIndex.map { x =>
        (mex.get(0).toString, x._1.toString, LogValueArray(x._2).toString.toDouble * 10)
      }
      // idValueSet.foreach(println)

    }.reduce((x, y) => x.union(y))

    // data.take(100).foreach(println)

    val sdata = MyContext.createDataFrame(data.toSeq)
      .toDF("srcsearchkey", "mediaid", "tfidf")
    //SampleDataOriginal.filter(col("srcsearchkey").===(lit("CFS"))).show(1000, false)
    //    sdata.filter(col("srcsearchkey").===(lit("CFS"))).show(1000, false)
    //    sdata.filter(col("srcsearchkey").===(lit("BX"))).show(1000, false)

    val joinData = SampleDataOriginal.join(sdata, Seq("srcsearchkey", "mediaid"))
      .select("score", "keyid", "rank", "mediaid", "lengthweight", "logplaytimes", "logsearchtimes", "new", "fee", "categoryweight", "tfidf", "srcsearchkey")
    //.filter(col("srcsearchkey").===(lit("CFS")))
    // joinData.show()

    val testRdd = ChangeMllibSampleData(joinData, featureStart, featureEnd)
    testRdd.take(20).foreach(println)
    println(testRdd.count())

    val dataSet = loadQueryDoc(testRdd)

    val LambdaData = buildLambda(dataSet)

    val trainingData = LambdaData


    // start training
    val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.setNumIterations(numIterations)
    boostingStrategy.treeStrategy.setMaxDepth(maxDepth)


    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)


    tempTestTree(model)

  }

  def ChangeRanklibSampleData(data: DataFrame, featureBegin: Int, featureEnd: Int): RDD[String] = {

    val dataRDD = data.rdd.map { ss =>
      val da1 = ss.get(0)
      val da2 = "qid:" + ss.get(1)
      val da3 = ss.get(2)
      var da4 = ""
      for (i <- featureBegin to featureEnd) {
        da4 = da4 + " " + (i - 3).toString + ":" + ss.get(i)
      }
      da1.toString + " " + da2.toString + " " + da4

    }
    dataRDD
  }


  /** *
    * Sample dataframe on hive to RDD
    */
  def ChangeMllibSampleData(data: DataFrame, featureBegin: Int, featureEnd: Int): RDD[String] = {

    val dataRDD = data.rdd.map { ss =>
      val da1 = ss.get(0)
      val da2 = "qid:" + ss.get(1)
      val da3 = ss.get(2)
      var da4 = ""
      for (i <- featureBegin to featureEnd) {
        da4 = da4 + " " + (i - 3).toString + ":" + ss.get(i)
      }
      da1.toString + " " + da2.toString + " " + da3.toString + da4

    }
    dataRDD
  }


  //train
  /** *
    * loadQueryDoc to mllib
    */
  def loadQueryDoc(Data: RDD[String], numFeatures: Int = -1): RDD[(String, Array[IndexItem])] = {
    val parsed = Data.map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#")))
      .map { line =>
        val items = line.split(" ")
        val y = items(0).toInt
        val qid = items(1).substring(4)
        val rank = items(2).toInt

        val (indies, values) = items.slice(3, items.length)
          .filter(_.nonEmpty)
          .map(_.split(":"))
          .filter(_.length == 2)
          .map { indexAndValue =>
            val index = indexAndValue(0).toInt
            val value = indexAndValue(1).toDouble
            (index, value)
          }.unzip

        (y, qid, rank, indies.toArray, values.toArray)
      }

    val d = if (numFeatures > 0) {
      numFeatures
    } else {
      parsed.persist(StorageLevel.MEMORY_AND_DISK)
      parsed.map(_._4.lastOption.getOrElse(0)).reduce(math.max)
    }

    parsed.map {
      case (y, qid, rank, indices, values) =>
        IndexItem(qid, rank, Vectors.sparse(d, indices, values), y)
    }.groupBy(_.qid)
      .mapValues(_.toArray)
      .persist(StorageLevel.MEMORY_AND_DISK)
  }

  def buildLambda(input: RDD[(String, Array[IndexItem])]): RDD[LabeledPoint] = {
    input.flatMap { case (qid, items) =>
      val scoreys = items.map { item =>
        (item.x, item.y)
      }
      val count = scoreys.map(_._2).sum

      val idealDCG = NDCG.idealDCG(count)
      val pseudoResponses = Array.ofDim[Double](scoreys.length)
      for (i <- pseudoResponses.indices) {
        val (_, yi) = scoreys(i)
        for (j <- pseudoResponses.indices if i != j) {
          val (_, yj) = scoreys(j)
          if (yi > yj) {
            val deltaNDCG = math.abs((yi - yj) * NDCG.discount(i) + (yj - yi) * NDCG.discount(j)) / idealDCG
            val rho = 1.0 / (1 + math.exp(0))
            val lambda = rho * deltaNDCG
            pseudoResponses(i) += lambda
            pseudoResponses(j) -= lambda
          }
        }
      }

      scoreys.zipWithIndex.map { case ((x, y), index) =>
        LabeledPoint(pseudoResponses(index), x)
      }
    }
  }

  def tempTestTree(model: GradientBoostedTreesModel): Unit = {
    val trees = model.trees
    val weights = model.treeWeights
    var output = "## LambdaMART\n"
    output += "## No. of trees = " + model.numTrees + "\n"
    output += "\n"
    output += "<ensemble>" + "\n"
    output += trees.zip(weights).zipWithIndex.foldLeft("") { case (result, ((tree, weight), index)) =>
      result + "\t<tree id=\"" + (index + 1) + "\" weight=\"" + weight + "\">" + "\n" + printTree(tree) + "\t</tree>" + "\n"

    }
    output += "</ensemble>\n"

    println(output)
  }

  def printTree(tree: DecisionTreeModel, indent: String = "\t\t"): String = {
    indent + "<split>" + "\n" +
      getNodeString(tree.topNode, indent + "\t") +
      indent + "</split>" + "\n"
  }

  def getNodeString(node: Node, indent: String = "\t\t"): String = {
    if (node.isLeaf) {
      indent + "<output> " + node.predict.predict + " </output>" + "\n"
    } else {
      indent + "<feature> " + node.split.get.feature + " </feature>" + "\n" +
        indent + "<threshold> " + node.split.get.threshold + " </threshold>" + "\n" +
        indent + "<split pos=\"left\">" + "\n" +
        getNodeString(node.leftNode.get, indent + "\t") +
        indent + "</split>" + "\n" +
        indent + "<split pos=\"right\">" + "\n" +
        getNodeString(node.rightNode.get, indent + "\t") +
        indent + "</split>" + "\n"
    }
  }
}
