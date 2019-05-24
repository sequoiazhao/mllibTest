package com.Working

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

/**
  * @author zhaoming on 2018-07-17 14:34
  **/
object TestWriteLocal {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)

    val sqlContext = SQLContext.getOrCreate(sc)

    val data = Array("12", "23", "34")
    //D:/usr/zm/train_data.txt
    //    val fileDir = new File("file://D:/data/train_data.txt")
    //    if(!fileDir.exists()){
    //      fileDir.createNewFile()
    //    }
    //    val writer = new PrintWriter(fileDir)
    //
    //    data.foreach(x =>
    //      writer.println(x)
    //    )
    //    writer.close()
    val sst = sc.parallelize(data)

    val result = sst.take(sst.count().toInt).mkString("\n")
    println(result)
  }

}
