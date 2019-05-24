package com.MLToolsTest.Normalizer

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

/**
  * @author zhaoming on 2018-01-11 11:35
  **/
object TestFilterLongWord {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)

    val sqlContext = SQLContext.getOrCreate(sc)
    val sentenceData = sqlContext.createDataFrame(Seq(
      (1, "狗 人民 社会 有一个 大家好 韩国 中华人民 奖励大家的事 是否需要这个元素".split(" "))
      , (2, "你好 很好 大家 德国 法国 社会问题 我 中华人民共和国 奖励大家的事 是否需要这个元素".split(" "))
    )).toDF("label", "sentence")

    sentenceData.show()

    //过滤掉其中较长的词，生成新列
    val convert = udf((array: Seq[String]) => array.filter(_.length.<=(2)).filter(_.length.>(1)).filterNot(_.contains("国")))
    val df = sentenceData.withColumn("new", convert(sentenceData.col("sentence")))
    df.foreach(println)


  }

}
