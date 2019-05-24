package Working

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * @author zhaoming on 2018-10-15 11:56
  **/
object MySqlTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[2]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.executor.memory", "8g")
      .set("spark.driver.memory", "8g")

    val sc = new SparkContext(conf)
    val MyContext = new HiveContext(sc)




    sc.setLocalProperty("spark.sql.warehouse.dir", "file:///D://code_test//mllibtest//spark-warehouse")

    //MyContext.sql("create database ltr")



    MyContext.sql("use ltr")

    //MyContext.sql("drop table o_unified_search_log_agg_history2")

    //    MyContext.sql("create table ltr.o_unified_search_log_agg_history2 (srcsearchkey string, agg string, score double )partitioned by (partitiondate string) row format delimited fields terminated by '|' stored as textfile")
    //
    //    MyContext.sql("load data local inpath 'd:/data/Historytable.txt' OVERWRITE INTO TABLE ltr.o_unified_search_log_agg_history2 partition(partitiondate='20181010')")

    val sdata = MyContext.sql("select * from ltr.o_unified_search_log_agg_history2 where partitiondate='20181010'")
    //    sdata.printSchema()
    //    sdata.show()
    //    sdata.show(false)

    val newdata = sdata.withColumn("length", size(split(col("agg"), "\\), \\(")))
      .select("length").take(1).apply(0)

    println(newdata.get(0).toString)

  }


}
