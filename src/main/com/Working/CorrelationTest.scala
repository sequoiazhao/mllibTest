package com.Working

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @author zhaoming on 2018-07-05 11:09
  **/
object CorrelationTest extends  Serializable{
  val columnx = new ListBuffer[Double]
  val columny = new ListBuffer[Double]
  val columnz = new ListBuffer[Double]
  val columnId = new ListBuffer[String]
  var colu: List[Double] = List()

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)


    // rdd
    val rowsRdd: RDD[Row] = sc.parallelize(
      Seq(
        Row("m1", 1, 2.0, 7.0, 1.0),
        Row("m2", 2, 3.5, 2.5, 2.0),
        Row("m3", 3, 7.0, 5.9, 1.0),
        Row("m4", 4, 1.0, 3.9, 1.0),
        Row("m5", 4, 1.0, 1.9, 1.0)
      )
    )

    // Schema
    val schema = new StructType()
      .add(StructField("media", StringType, true))
      .add(StructField("id", IntegerType, true))
      .add(StructField("item_1", DoubleType, true))
      .add(StructField("item_2", DoubleType, true))
      .add(StructField("item_3", DoubleType, true))

    // Data frame
    val df = sqlContext.createDataFrame(rowsRdd, schema)
    val df2 = df.select("item_1", "item_2", "item_3", "media")

    val rows = new VectorAssembler()
      .setInputCols(df2.select("item_1", "item_2", "item_3").columns).setOutputCol("corr_features")
      .transform(df2)
      .select("corr_features", "media")
      .rdd
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)


    val ss = rows.foreach { x =>

      val y: DenseVector = x.get(0).asInstanceOf[DenseVector]
      //println(x.get(1))

      columnId.append(x.get(1).toString)
      columnx.append(y.apply(0))
      columny.append(y.apply(1))
      columnz.append(y.apply(2))

    }

    println(columnx)
    println(columny)
    println(columnz)

    val sRdd = sc.parallelize(Seq(
      Vectors.dense(columnx.toArray),
      Vectors.dense(columny.toArray),
      Vectors.dense(columnz.toArray)))

    val correl: Matrix = Statistics.corr(sRdd, "pearson")
    println(correl)





    // rdd
    val rowsRdd2: RDD[Row] = sc.parallelize(
      Seq(
        Row("m1", 1, Array(2.0, 7.0, 1.0)),
        Row("m2", 2, Array(3.5, 2.5, 2.0)),
        Row("m3", 3, Array(7.0, 5.9, 1.0)),
        Row("m4", 4, Array(1.0, 3.9, 1.0)),
        Row("m5", 4, Array(1.0, 1.9, 1.0))
      ))

    // Schema
    val schema2 = new StructType()
      .add(StructField("media", StringType, true))
      .add(StructField("id", IntegerType, true))
      .add(StructField("item_1", ArrayType(DoubleType), true))

    // Data frame
    val dfx2 = sqlContext.createDataFrame(rowsRdd2, schema2).persist(StorageLevel.MEMORY_ONLY)
    dfx2.show()
    //rowsRdd2
    //method 2

    val seriesX: RDD[Double] = sc.parallelize(Array(1, 2, 3, 3, 5))


    val stttt = dfx2.map{x=>

    val ssxx=x.get(2).asInstanceOf[mutable.WrappedArray[Double]].toArray

    val sx :RDD[Double]= sc.makeRDD(ssxx)
      (1,2,3)
    }
    stttt
//    dfx2.count()
    dfx2.rdd.map{x=>
      x
    }



  }

}
