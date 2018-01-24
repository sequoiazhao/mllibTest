package com.MLTest

import org.apache.spark.ml.feature.PCA
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext

/**
  * @author zhaoming on 2018-01-15 14:54
  **/
object PCATest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(5, 10, 1,5,6),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val pca = new  PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)
      .fit(df)

    val result = pca.transform(df).select("pcaFeatures")
    result.show(false)

  }

}
