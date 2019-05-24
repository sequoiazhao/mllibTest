package com.AllTest.BigDL


import com.intel.analytics.bigdl.nn._
import com.intel.analytics.bigdl.nn.mkldnn.SpatialConvolution
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.utils.Engine
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

/**
  * @author zhaoming on 2018-02-09 9:31
  **/
object TestFirst {

  def main(args: Array[String]): Unit = {
    //v1.0 config
    val sparkConf = Engine.createSparkConf()
    sparkConf.setAppName("MyTest")
      .setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
      // .set("spark.driver.allowMultipleContexts", "true")
      //  .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.executor.memory", "4g")
      //.set("spark.port.maxRetries", "100")
      .set("spark.shuffle.reduceLocality.enabled", "false")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.scheduler.minRegisteredResourcesRatio", "1.0")
      .set("spark.speculation", "false")
    //.set("bigdl.localMode","true")
    //.set("spark.shuffle.service.enabled","true")

    //
    //    System.setProperty("bigdl.localMode", "true")
    //    System.setProperty("bigdl.coreNumber", "2")


    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    Engine.init
    val ss = Tensor[Double](2, 2).fill(1.0)
    println(ss)


  }


}
