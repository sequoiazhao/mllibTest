package com.SearchLtr

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhaoming on 2018-04-12 14:49
  **/
object Test {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val MyContext = new HiveContext(sc)

    val mdd = sc.textFile("D:\\dataframeData\\englocal01")

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val datardd = mdd.map { x =>
      val s1 = x.replace("[", "").replace("]", "").split(",")
      (s1(0), s1(1), s1(2), s1(3), s1(4), s1(5), s1(6), s1(7), s1(8), s1(9), s1(10))
    }
    val relDataAll = datardd.toDF("score", "keyid", "rank", "mediaid", "lengthweight", "logplaytimes", "logsearchtimes", "new", "fee", "categoryweight", "srcsearchkey")
      .withColumn("score2", floor(log(col("score").+(lit(1.7183)))))
      .drop("score")
      .select(col("score2").as("score")
        , col("keyid")
        , col("rank")
        , col("mediaid")
        , col("lengthweight")
        , col("logplaytimes"))


    //relDataAll.select("srcsearchkey", "mediaid", "score2").show(relDataAll.count().toInt, false)
    relDataAll.show(2000)

    val testRdd = ChangeMllibSampleData(relDataAll, 4, 5)
    // testRdd.take(20).foreach(println)
    // println(testRdd.count())

    val dataSet = loadQueryDoc(testRdd)

    //dataSet.take(100).foreach(x=>x._2.foreach(println))

    //val LambdaData = buildLambda(dataSet)
    println(dataSet.count())

    val sss = dataSet.flatMap {

      case (qid, items) =>
        val scoreys = items.map { item =>
          //println(item.y)
          (item.x, item.y)
        }

        val count = scoreys.map(_._2).sum
        println(count)
        val idealDCG = NDCG.idealDCG(count)
        println(idealDCG)
        println(scoreys.length)

        val pseudoResponses = Array.ofDim[Double](scoreys.length)
        println("indices" + pseudoResponses.indices)

        for (i <- pseudoResponses.indices) {
          val (_, yi) = scoreys(i)
          for (j <- pseudoResponses.indices if i != j) {
            val (_, yj) = scoreys(j)
            if (yi > yj) {
              val deltaNDCG = math.abs((yi - yj) * NDCG.discount(i) + (yj - yi) * NDCG.discount(j)) / idealDCG
              //println("deltaNDCG"+deltaNDCG)
              val rho = 1.0 / (1 + math.exp(0))
              val lambda = rho * deltaNDCG
              //println("lambda" + lambda)
              pseudoResponses(i) += lambda
              pseudoResponses(j) -= lambda
            }
          }
        }
        pseudoResponses.foreach(println)

        scoreys.zipWithIndex.map{case((x,y),index)=>
        LabeledPoint(pseudoResponses(index),x)
        }

    }
    sss.count()


    sss.foreach(x=>println(x))


    //    val tdd = sc.textFile("D:\\dataframeData\\local02")
    //
    //    // tdd.foreach(println)
    //
    //    val relData = tdd.map { x =>
    //      val s1 = x.replace("[", "").replace("]", "").replace(", ", ":").split(",")
    //      (s1(0), s1(1))
    //    }.toDF("srcsearchkey", "mediaid")
    //  }

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
      //println("count"+count)

      val idealDCG = NDCG.idealDCG(count)
      //println("idealDCG"+idealDCG)

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


}
