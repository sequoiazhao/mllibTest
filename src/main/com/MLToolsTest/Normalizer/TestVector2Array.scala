package com.MLToolsTest.Normalizer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.rdd.RDD

/**
  * @author zhaoming on 2018-01-10 14:57
  **/
object TestVector2Array {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    // The original data.
    val input: DataFrame =
      sc.parallelize(1 to 4)
        .map(i => i.toDouble -> new DenseVector(Array(i.toDouble * 2)))
        .toDF("id", "dist")
    input.show()
    input.printSchema()

    // Turn it into an RDD for manipulation.
    val inputRDD: RDD[(Double, DenseVector)] =
      input.map(row => row.getAs[Double]("id") -> row.getAs[DenseVector]("dist"))

    // Change the DenseVector into an integer array.
    val outputRDD: RDD[(Double, Array[Int])] =
      inputRDD.mapValues(_.toArray.map(_.toInt))


    // Go back to a DataFrame.
    val output = outputRDD.toDF("id", "dist")
    output.show
    output.printSchema()
  }

}
