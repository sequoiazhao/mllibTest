package com.MLTest

import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhaoming on 2018-01-18 8:59
  **/
object RFormulaTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RFormula-Test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    var sqlContext = new SQLContext(sc)

    val dataset = sqlContext.createDataFrame(Seq(
      (7, "US", 18, 1.0,"a"),
      (8, "CA", 12, 0.0,"b"),
      (9, "NZ", 15, 0.0,"a")
    )).toDF("id", "country", "hour", "clicked","my_test")

    dataset.show()
    val formula = new RFormula()
      .setFormula("clicked ~ country + hour + my_test")
      .setFeaturesCol("features")
      .setLabelCol("label")
    val output = formula.fit(dataset).transform(dataset)
    output.show()
    //output.select("features", "label").show()

  }

}
