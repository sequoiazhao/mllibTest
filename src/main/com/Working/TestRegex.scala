package com.Working

/**
  * @author zhaoming on 2018-06-27 13:36
  **/


object TestRegex {

  def main(args: Array[String]): Unit = {
    val str = "奥斯卡提名奖 ,中国 ,你好 ,百花奖金提名, 奖获得者, ABCji奖 ,奥斯卡票房榜,电影节,情节,五四青年节,年代戏 ，80年代;你好 ，什么；再说;哈,10-13岁,29岁"

    val ssst = str.replaceAll("([\u4e00-\u9fa5_a-zA-Z0-9]+)?奖([\u4e00-\u9fa5_a-zA-Z0-9]+)?", "")
      .replaceAll("([\u4e00-\u9fa5_a-zA-Z0-9]+)?榜([\u4e00-\u9fa5_a-zA-Z0-9]+)?", "")
      .replaceAll("[\u4e00-\u9fa5_a-zA-Z0-9]{2,}年代","")
      .replaceAll("[\u4e00-\u9fa5_a-zA-Z0-9]{2,}节","")
      .replaceAll("([\u4e00-\u9fa5_a-zA-Z0-9]+)?(-)?([\u4e00-\u9fa5_a-zA-Z0-9]+)?岁", "")
    val ssst2 = ssst.split("[,]|[;]|[，]|[；]").filter(p => p != "").filterNot(p => p.matches("[ ]+"))

    ssst2.foreach(println)

    val regex = ("([\u4e00-\u9fa5_a-zA-Z0-9]+)?榜([\u4e00-\u9fa5_a-zA-Z0-9]+)?|" +
      "([\u4e00-\u9fa5_a-zA-Z0-9]+)?奖([\u4e00-\u9fa5_a-zA-Z0-9]+)?|" +
      "[\u4e00-\u9fa5_a-zA-Z0-9]{2,}年代|" +
      "([\u4e00-\u9fa5_a-zA-Z0-9]+)?(-)?([\u4e00-\u9fa5_a-zA-Z0-9]+)?岁|"+
      "[\u4e00-\u9fa5_a-zA-Z0-9]{2,}节").r
    val ssst3 = regex.findAllMatchIn(str).map(_.toString).toSeq

    val strx = "86100300900000100000060a48ca376e"

    val regx ="86100.*[a-e]$".r
     val res = regx.findAllMatchIn(strx).map(_.toString).toSeq
    res.foreach(println)


//    println("")
    //    println("")
    ssst3.foreach(println)

    val str2 ="1110, 101gou, 100你好, 好100, adfa100,fw22fw"

//    val rdx = "([0-9]+)?[a-zA-Z-:_ ./|=&?]+".r
   val rdx = "[\\w]+".r

    val ttt = rdx.findAllIn(str2).map(_.toString).toSeq
    ttt.foreach(println)

    val china ="[\u4e00-\u9fa5]+".r

    val ddd = china.findAllIn(str2).map(_.toString).toSeq
    ddd.foreach(println)

    val stttr ="('fwfwefwef')"

    println(stttr.replaceAll("List|\\(|\\)|\"",""))


    //    val conf = new SparkConf().setAppName("LDATest").setMaster("local[*]")
    //      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    //    val sc = new SparkContext(conf)
    //    //val MyContext = new HiveContext(sc)
    //    //val mdd = sc.textFile("D:\\dataframeData\\englocal01")
    //
    //    val sqlContext = SQLContext.getOrCreate(sc)
    //    import sqlContext.implicits._
    //    val ratings = sc.textFile("D:\\data\\sample_movielens_ratings.txt")
    //      .map(parseRating)
    //      .toDF()
    //
    //    ratings.show()
    //
    //    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
    //
    //    // Build the recommendation model using ALS on the training data
    //    val als = new ALS()
    //      .setMaxIter(5)
    //      .setRegParam(0.01)
    //      .setUserCol("userId")
    //      .setItemCol("movieId")
    //      .setRatingCol("rating")
    //    val model = als.fit(training)
    //
    //    val predictions = model.transform(test)
    //
    //    val evaluator = new RegressionEvaluator()
    //      .setMetricName("rmse")
    //      .setLabelCol("rating")
    //      .setPredictionCol("prediction")
    //    val rmse = evaluator.evaluate(predictions)
    //    println(s"Root-mean-square error = $rmse")
    //    model.itemFactors.show(20)
    //
    //    predictions.show(false)
    //
    //    println(model.rank)
    //
    //
    //    // MyContext.clearCache()
    // sc.stop()

    val sssee ="单曲"

    val filterRegex2 = "([\u4e00-\u9fa5_a-zA-Z0-9]+)?"

    //println(filterRegex2.(sssee))
  }

}
