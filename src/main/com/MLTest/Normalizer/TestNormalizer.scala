package com.MLTest.Normalizer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{MinMaxScaler, Normalizer, StandardScaler}
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{ArrayType, DoubleType}
import org.apache.spark.sql.functions._


/**
  * @author zhaoming on 2018-01-10 10:11
  **/
object TestNormalizer {
  def main(args: Array[String]): Unit = {

    //增加此条目确保HiveContext可以用，如果没有会有一个tmp报错信息，一直没有解决
    Logger.getLogger("org").setLevel(Level.FATAL)
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)

    val sqlContext = SQLContext.getOrCreate(sc)

    val hiveContext = new HiveContext(sc)

    val df = hiveContext.createDataFrame(Seq(
//      (0.0, Vectors.dense(1.0, 0.5, -1.0)),
//      (1.0, Vectors.dense(2.0, 1.0, 1.0)),
//      (2.0, Vectors.dense(4.0, 10.0, 2.0))
        (0.0, Array(1.0, 0.5, -1.0)),
      (1.0, Array(2.0, 1.0, 1.0)),
      (2.0, Array(4.0, 10.0, 2.0))
    )).toDF("id", "features")
    //dataFrame.show()
    import sqlContext.implicits._
     val converToVec = udf((array:Seq[Double])=>
     Vectors.dense(array.toArray))

    val dataFrame =df.withColumn("hashValues",converToVec($"features"))
    df.printSchema()
    dataFrame.show()

    //Normalizer
    val normalizer =new Normalizer()
      .setInputCol("hashValues")
      .setOutputCol("normFeatures")
      .setP(1.0)

    //正则化一阶范数
    val NormData = normalizer.transform(dataFrame)
//    val ss = NormData.col("normFeatures")
//    val bb =NormData.withColumn("ss",ss)
//    bb.printSchema()

    val vectorToColumn = udf((x:DenseVector)=>x.toArray)

    NormData.withColumn("firstValue",vectorToColumn($"normFeatures")).show()

     NormData.drop("hashValues").drop("firstValue").show()



//    val inputRDD: RDD[(Double, DenseVector)] =
//      NormData.map(row => row.getAs[Double]("id") -> row.getAs[DenseVector]("normFeatures"))
//
//    // Change the DenseVector into an integer array.
//    val outputRDD: RDD[(Double, Array[Double])] =
//      inputRDD.mapValues(_.toArray)
//
//    // Go back to a DataFrame.
//
//    val output = outputRDD.toDF("id", "dist")
//  output.show()
//    //正则化无穷阶范数
//    val InfNormData = normalizer.transform(dataFrame,normalizer.p->Double.PositiveInfinity)
//    InfNormData.show()
//
//
//    //StandardScaler每一列的标准差为1
//    val scaler = new StandardScaler()
//      .setInputCol("features")
//      .setOutputCol("scaledFeatures")
//      .setWithStd(true)
//      .setWithMean(false)
//
//    val scalerModel = scaler.fit(dataFrame)
//
//    val scaledData = scalerModel.transform(dataFrame)
//    scaledData.foreach(println)
//
//    // MinMaxScaler 也是对列进行操作
//    val Mscaler = new MinMaxScaler()
//      .setInputCol("features")
//      .setOutputCol("scaledFeatures")
//
//    val MscalerModel = Mscaler.fit(dataFrame)
//
//    val MscaledData = MscalerModel.transform(dataFrame)
//    MscaledData.foreach(println)



  }

}
