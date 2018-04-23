import java.io.File

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * @author zhaoming on 2017-12-18 13:43
  **/
object TestMain {
  def main(args: Array[String]): Unit = {
    //    println(System.getProperty("user.dir"))
    //
    //    val directory = new File("")
    //
    //    println(directory.getCanonicalPath)
    //    println(directory.getAbsolutePath)

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)


    val events = sc.parallelize(
      """{"action":"create","timestamp":1452121277}""" ::
        """{"action":"create","timestamp":"1452121277"}""" ::
        """{"action":"create","timestamp":""}""" ::
        """{"action":"create","timestamp":null}""" ::
        """{"action":"create","timestamp":"null"}""" ::
        Nil
    )



    val schema = (new StructType).add("action", StringType).add("timestamp", StringType)

    val ss = sqlContext.read.schema(schema)
      .json(events)
      .select(col("action"),
        when(col("timestamp").cast(LongType).isNull, lit(-1000))
          .otherwise(col("timestamp").cast(LongType)).as("timestamp"))
    //    val ss2 = ss.withColumn("new",  when(col("timestamp").cast(LongType).isNull,lit(-1000)).otherwise(col("timestamp").cast(LongType)))
    //    ss2.show()
    //ss.show()
    val sqlContextx = SQLContext.getOrCreate(sc)
    import sqlContextx.implicits._
    val sss = events.toDF("ddd")
//
//    sss.show()

    val sentenceData = sqlContext.createDataFrame(Seq(
     // (1, "狗 人民 社会 有一个 大家好 韩国 中华人民 奖励大家的事 是否需要这个元素".split(" "))
      //, (2, "你好 很好 大家 德国 法国 社会问题 我 中华人民共和国 奖励大家的事 是否需要这个元素".split(" "))
      (1,Seq(2,1,3))

    )).toDF("label", "sentence")

//    val df_new = df.select(column_names_col: *)
//    df_new.show()
    val df =sentenceData.select("sentence.*")
    df.show()



  }


}
