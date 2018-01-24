package com.MLTest

import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * @author zhaoming on 2018-01-18 9:04
  **/
object ChiSqSelectorTest {

  case class Bean(id: Double, features: org.apache.spark.mllib.linalg.Vector, clicked: Double) {}


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)


    val data = Seq(
      (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
      (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
      (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
    )

    val beanRDD = sc.parallelize(data).map(t3 => Bean(t3._1, t3._2, t3._3))
    val df = sqlContext.createDataFrame(beanRDD)

    val selector = new ChiSqSelector()
      .setNumTopFeatures(2)
      .setFeaturesCol("features")
      .setLabelCol("clicked")
      .setOutputCol("selectedFeatures")

    val result = selector.fit(df).transform(df)
    result.show()

  }

}
