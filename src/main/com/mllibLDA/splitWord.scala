package com.mllibLDA

import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, row_number}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.functions._

/**
  * @author zhaoming on 2018-01-19 9:18
  **/
abstract class splitWord {

  //spark
  Logger.getLogger("org").setLevel(Level.FATAL)
  val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
    .set("spark.sql.warehouse.dir", "/spark-warehouse/")
  val sc = new SparkContext(conf)
  val sqlcon: SQLContext = new HiveContext(sc)


  /** *
    * 对文本进行分词，采用ansj
    *
    * @param inPath
    * @param Length 分词后取数据集的长度
    * @return
    */
  def getSplitWordsDF(inPath: String, Length: Int): DataFrame = {

    //读入文件
    val hadoopConf = sc.hadoopConfiguration
    val fs = new Path(inPath).getFileSystem(hadoopConf)
    val len = fs.getContentSummary(new Path(inPath)).getLength / (1024 * 1024)
    val minPart = (len / 32).toInt

    //数据标记
    val data = sc.textFile(inPath, minPart).zipWithIndex().map(_.swap)

    //分词
    val resultRDD = new PreUtils().run(data)

    //组合DataFrame
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val tokenDFx = resultRDD.toDF("idx", "tokens")

    val tokenDF = tokenDFx.withColumn("id", lit(row_number() over Window.orderBy("tokens")).cast(LongType)).limit(Length)
      .drop("idx")

    val convert = udf((array: Seq[String]) => array.filter(_.length.>=(2)))
    val dfFilter = tokenDF.withColumn("new_tokens", convert(tokenDF.col("tokens")))

    val getResult = dfFilter.drop("tokens").withColumnRenamed("new_tokens", "tokens")

    getResult

  }

}
