package Working

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhaoming on 2018-10-15 11:58
  **/
object MyContext {


  /**
    * Created by chengdianhu on 2017/3/13.
    */
  var sc: SparkContext = _
  var sqlContext: HiveContext = _

  def init() = {
    val sparkConf = new SparkConf().setAppName("User mining").setMaster("local[*]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    sc = new SparkContext(sparkConf)
    sqlContext = new HiveContext(sc)
  }


}
