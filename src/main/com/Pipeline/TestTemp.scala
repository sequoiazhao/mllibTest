package com.Pipeline

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhaoming on 2018-01-15 13:57
  **/
object TestTemp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)

    val df = sc.parallelize(Seq("Ahir", "Pat", "Andy", "Pfwe", "Andfw"))
      .distinct(numPartitions = 6)
      .map(name => (name.charAt(0), 1))
      //      .groupByKey()
      //      .mapValues(names => names.size)
      .reduceByKey(_ + _)
      .collect()
    df.foreach(println)


  }

}
