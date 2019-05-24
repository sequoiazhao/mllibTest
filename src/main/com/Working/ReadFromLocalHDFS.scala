package Working

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * @author zhaoming on 2018-10-18 11:58
  **/
object ReadFromLocalHDFS {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[2]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.executor.memory", "8g")
      .set("spark.driver.memory", "8g")

    val sc = new SparkContext(conf)

    val rdds = sc.textFile("hdfs://192.168.32.128//user/root/linkage/block_1.csv")
    println(rdds.count())
    rdds.take(10).foreach(println)

  }
}
