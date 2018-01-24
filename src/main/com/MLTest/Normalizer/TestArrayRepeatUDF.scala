package com.MLTest.Normalizer

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * @author zhaoming on 2018-01-17 10:32
  **/
object TestArrayRepeatUDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)

    val sqlContext = SQLContext.getOrCreate(sc)

    val ArrayTest = Array("12", "你好", "中", "中国","abc", "2012", "天下太不怕")

    val regex = "[\\u4E00-\\u9FA5]|-?([0-9]+).([0-9]+)|-?([0-9]+)|([a-z]+)"

    if (ArrayTest !=null) {
      val filterWord = ArrayTest.filter(x => x.matches(regex).!=(true))
      //filterWord.foreach(println)

      //重复filterWord
     val newt = filterWord.map(x=>
        (1 to 3).map(_=>x)
       )
      val newt2 = newt.flatMap(List.tabulate(3)(_))

    }

  }


}
