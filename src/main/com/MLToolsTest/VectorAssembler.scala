package com.MLToolsTest

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @author zhaoming on 2018-01-15 15:05
  **/
object VectorAssembler {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val dataset = sqlContext.createDataFrame(
      Seq((0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")
    dataset.show(false)

    val assembler = new VectorAssembler()
      .setInputCols(Array("hour", "mobile", "clicked"))
      .setOutputCol("features")

    val output = assembler.transform(dataset)

    output.show(false)

    output.printSchema()
  }

}
