package com.Working

import com.AllTest.mllibLDA.splitWord
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, UserDefinedFunction}
import org.apache.spark.storage.StorageLevel

/**
  * @author zhaoming on 2018-01-09 13:38
  **/
object LDAandWord2Vec extends splitWord {
  val inPath = "D:/code_test/mllibtest/data/train"

  val vecModelPath = "D:/code_test/mllibtest/model"
  val ldaModelPath = "D:/code_test/mllibtest/model/ldaModel"


  val colCorr: UserDefinedFunction = udf((vc1: DenseVector, vc2: DenseVector) => {
    val series1: RDD[Double] = sc.parallelize(vc1.toArray)
    val series2: RDD[Double] = sc.parallelize(vc2.toArray)
    Statistics.corr(series1, series2, "pearson")
  })

  def main(args: Array[String]): Unit = {

    //    val sst = args(0)
    //    println(sst)

    val splitWords = getSplitWordsDF(inPath, 15)
    //splitWords.show(false)

    //1、调用word2Vec
    val word2Vec = new Word2Vec()
      .setInputCol("tokens")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(1)

    val model = word2Vec.fit(splitWords)

    val result = model.transform(splitWords)

    result.select("result").take(10).foreach(println)


    //val vec = Vector("世界杯","清华大学")


    val syn = model.findSynonyms("世界杯", 5)


    // syn.show()


    //需要测试一下syn在大数据集上的效果，看能否找到相似词

    //model.getVectors.show(false)
    //    val s1 = model.getVectors.filter(col("word").===("世界杯")).select("vector")
    //    val s2 = model.getVectors.filter(col("word").===("样品")).select("vector")
    //
    //    val a11 = s1.take(1).map(_.get(0)).mkString
    //      .replaceAll("(\\[)|(\\])", "").split(",").map(_.toDouble)

    //
    //    val a1:Array[Double] =s1.take(1).map((x=>x.toArray)
    //    val a2 = s2.take(1).map(_.get(0).asInstanceOf[Double])


    //    val a1 = s1.take(1).map(x => x.get(0)).mkString
    //      .replaceAll("(\\[)|(\\])", "").split(",").map(_.toDouble)
    //
    //    val a2 = s2.take(1).map(x => x.get(0)).mkString
    //      .replaceAll("(\\[)|(\\])", "").split(",").map(_.toDouble)


    val st1 = Array("世界杯", "样品", "节奏", "步枪")
    val st2 = "清华大学"

    //    model.getVectors.where(col("word").===("世界杯")).show(false)

    val dataDF = model.getVectors
      .withColumn("id", row_number().over(Window.orderBy("word")))
      .persist(StorageLevel.MEMORY_ONLY)

    dataDF.show()

    val corrVal = st1.map { x =>
      val series1: RDD[Double] = sc.parallelize(getVector(dataDF, x))
      val series2: RDD[Double] = sc.parallelize(getVector(dataDF, st2))
      (x, st2, Statistics.corr(series1, series2, "pearson"))
    }


    //val vectorToColumn = udf((x: DenseVector) => x.toArray)

    val dataDDF = dataDF.join(dataDF
      .withColumnRenamed("word", "word2")
      .withColumnRenamed("vector", "vector2")
      .withColumnRenamed("id", "id2"))
      .filter(col("id") > col("id2"))
    //      .withColumn("array1",vectorToColumn(col("vector")))
    //      .withColumn("array2",vectorToColumn(col("vector")))


    dataDDF.show()



    val newData = dataDDF.withColumn("new", colCorr(dataDDF.col("vector"), dataDDF.col("vector2")))
    newData.show()

    corrVal.foreach(println)

    //    for (i <- 1 to 10) {
    //      val series1: RDD[Double] = sc.parallelize(a1)
    //      val series2: RDD[Double] = sc.parallelize(a2)
    //      val correlation: Double = Statistics.corr(series1, series2, "pearson")
    //      println(correlation)
    //    }


    // println(model.getVectors.getClass)

    //    model.getVectors.printSchema()
    //
    //  val ssstx =  model.getVectors.select("vector")
    //
    //    val st = ssstx.map(row=>row.get(0).asInstanceOf[Double])
    //    st.foreach(println)

    //println(st.count)


    //val correlMatrix =Statistics.corr(st,"pearson")
    //
    //    println(correlMatrix.numRows,correlMatrix.numCols)
    //
    //    println(correlMatrix.apply(1,2))
    // correlMatrix.toArray.foreach(println)
    //
    //
    //    val seriesX: RDD[Double] = sc.parallelize(Array(1, 2, 3, 3, 5))  // a series
    //    // must have the same number of partitions and cardinality as seriesX
    //    val seriesY: RDD[Double] = sc.parallelize(Array(11, 22, 33, 33, 555))
    //
    //    val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")
    //    println(s"Correlation is: $correlation")
    //
    //    val data: RDD[Vector] = sc.parallelize(
    //      Seq(
    //        Vectors.dense(1.0, 10.0, 100.0),
    //        Vectors.dense(2.0, 20.0, 200.0),
    //        Vectors.dense(5.0, 33.0, 366.0),
    //        Vectors.dense(15.0, 3.0, 66.0))
    //    )  // note that each Vector is a row and not a column
    //
    //    // calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method
    //    // If a method is not specified, Pearson's method will be used by default.
    //    val correlMatrix: Matrix = Statistics.corr(data, "pearson")
    //    println(correlMatrix.toString)
    //
    //
    //   val sssu =  data.cartesian(data)
    //    sssu.foreach(println)


    val tagStr = "爱情"
    val tagTemp = "女人"
    val tagArray = List("爱情剧", "科幻片")
    sss(tagStr, tagTemp, tagArray)


  }

  def sss(tagStr: String, tagTeamp: String, tagArray: List[String]): Unit = {
    tagArray.foreach { x =>
     println( x.replaceAll("([\u4e00-\u9fa5][\u4e00-\u9fa5])剧", "$1"))

    }
  }

  def getVector(dataDF: DataFrame, str: String): Array[Double] = {
    dataDF.where(col("word").===(str)).select("vector").take(1).map(_.get(0)).mkString
      .replaceAll("(\\[)|(\\])", "").split(",").map(_.toDouble)
  }

  def getValue(s: Double): Double = {
    s * 2
  }


}
