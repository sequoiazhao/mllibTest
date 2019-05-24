package com.MLToolsTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhaoming on 2018-01-18 8:25
  **/
object VectorSlicer {
  def main(args: Array[String]): Unit = {

    //从一个列的集合里取出组成另一个列，特征选取
    //    +----------------------+--------------+
    //    |userFeatures          |features      |
    //    +----------------------+--------------+
    //    |[-2.0,2.3,0.0,1.0,2.0]|[2.3,2.0,-2.0]|
    //    +----------------------+--------------+

    Logger.getLogger("org").setLevel(Level.FATAL)
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    //val sqlcon: SQLContext = new HiveContext(sc)
    var sqlContext = new SQLContext(sc)
    println("dddd")

    val data = Array(Row(Vectors.dense(-2.0, 2.3, 0.0, 1.0, 2.0)))

    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3", "f4", "f5").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])


    val dataRDD = sc.parallelize(data)

    val dataset = sqlContext.createDataFrame(dataRDD, StructType(Array(attrGroup.toStructField())))
    dataset.show(false)

    val slicer = new VectorSlicer()
      .setInputCol("userFeatures")
      .setOutputCol("features")

    slicer.setIndices(Array(1)).setNames(Array("f5", "f1"))

    val output = slicer.transform(dataset)
    output.show(false)
    println(output.select("userFeatures", "features").first())
  }


}
