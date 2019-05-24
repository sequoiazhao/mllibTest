package search

/**
  * @author zhaoming on 2018-04-18 16:22
  **/
object TestStringfilter {

  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
    //      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    //    val sc = new SparkContext(conf)
    //    val MyContext = new HiveContext(sc)
    //
    //
    //    val sss ="你好,123"
    //    val tt = sss.split(",")
    //    println(tt(1))S
    val baseExpr =
    """[^\u4e00-\u9fa5]""".r

//    val wordFilter = baseExpr.replaceAllIn(sentence, "")

    var str =""","value":0.74},{"name":"大片","value":0.77},{"name":"动作归片","value":0.56},{"name":"剧情片","value":0.73},{"name":"片花","value":1.11},{"name":"残酷","value":0.38},{"name":"鬼片","value":1.0},{"name":"恐怖片","value":0.48},{"name":"惊悚片","value":0.32},,{"name":"言情剧","value":0.48},{"name":"古装剧","value":0.32},{"name":"自己","value":0.1},{"name":"鬼魂","value":0.94},{"name":"小镇","value":0.69}]"""

    println(str.replaceAll("([\u4e00-\u9fa5][\u4e00-\u9fa5])片","$1")
      .replaceAll("([\u4e00-\u9fa5][\u4e00-\u9fa5])剧","$1"))
  }

}
