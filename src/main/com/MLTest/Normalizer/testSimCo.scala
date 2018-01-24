package com.MLTest.Normalizer


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType}

import scala.collection.mutable.ArrayBuffer

/**
  * @author zhaoming on 2018-01-03 17:49
  **/
object testSimCo {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.FATAL)
    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
//    sqlContext.table("ltvrs_test.t_epg_media_sum").show()
//    sqlContext.table("vodrs.t_media_sum").show()



    //用schema 生成dataframe
    //    val schema = StructType(List(
    //      StructField("score", DoubleType, nullable = true)
    //    ))
    //
    //    //插入数据的内容
    //    val rdd = sc.parallelize(Seq(
    //      Row(101.0)
    //      , Row(22.1)
    //      , Row(45.9)
    //    ))
    //    val algData = sqlContext.createDataFrame(rdd, schema)
    //    algData.show()

    //用隐式转换 生成dataframe
    val a = Array((1,Seq("ha"),Seq(1, 2, 3),Seq(2,4,5)),(2,Seq("ha"),Seq(2,3,1), Seq(2, 3, 4)), (3,Seq("ba"),Seq(3,2,1),Seq(5, 6, 7)), (4,Seq("ba"),Seq(2,3,1),Seq(8, 9, 10)))

    import sqlContext.implicits._
    val vender = sc.makeRDD(a).toDF("id","word","topic","score")
    val venderself = sc.makeRDD(a).toDF("id2","word","topic2","score2")
    //val joindata = vender.join(broadcast(venderself)).filter(col("word").===(col("word2")))
    val joindata =vender.join(broadcast(venderself),Seq("word"))
    joindata.show()

    val udfCompute = udf(funCompute _)

//    val columnIntersection = udfCompute(vender.col("score").cast(ArrayType(DoubleType))
//    ,venderself.col("score2").cast(ArrayType(DoubleType)))


    val columnIntersection = udfCompute2(vender.col("score").cast(ArrayType(DoubleType))
      ,venderself.col("score2").cast(ArrayType(DoubleType)),vender.col("topic").cast(ArrayType(IntegerType))
    ,venderself.col("topic2").cast(ArrayType(IntegerType)))

    val dataIns = joindata.withColumn("newscore",columnIntersection)

    //dataIns.show()

    val columnRank = row_number().over(Window
      .partitionBy(dataIns.col("id"))
      .orderBy(dataIns.col("newscore").desc))

    dataIns.withColumn("ttt",columnRank).show()


  }

  def funCompute(left: Seq[Double], right: Seq[Double]): Double = {
   // left.intersect(right)
    //val k:Seq[Double] = _

    //归一化


    val k = ArrayBuffer[Double]()
    for(i<-0 to left.length-1){

    k+=math.pow((left(i)-right(i)),2)
    }
    k.reduce(_+_)

  }


  def funCompute2(left: Seq[Double], right: Seq[Double],ltopic:Seq[Int],rtopic:Seq[Int]): Double = {
    val k = ArrayBuffer[Double]()
    println(left.length,ltopic.length)
    for(i<-left.indices){
      //按id找计算的对应right值
     for(j<-ltopic.indices){
       if (rtopic(j)==ltopic(i)){
         k+=math.pow(left(i)-right(j),2)
       }
     }

    }
    k.sum
  }
  val udfCompute2 = udf(funCompute2 _)
}
//    val s1 = explode(vender.col("score"))
//
//    val vender2 = vender.withColumn("ss",s1)
//    vender2.show()



//生成一个全部的矩阵关系

//val pred_ = IndexedRowMatrix(Pred_Factors.rdd.map(lambda x: IndexedRow(x[0],x[1]))).toBlockMatrix().transpose().toIndexedRowMatrix()

//val pred =
// val pred_sims = pred.columnSimilarities()