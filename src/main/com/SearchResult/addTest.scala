package com.SearchResult

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * @author zhaoming on 2018-05-02 17:50
  **/
object addTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val MyContext = new HiveContext(sc)

    //val mdd = sc.textFile("D:\\dataframeData\\englocal01")

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val s1 = Array(("宝莱坞", 1.0), ("同性", 1.0), ("同性恋", 1.0), ("小人物", 1.0), ("18禁", 1.0), ("歌舞", 1.0), ("文艺", 1.0), ("人性", 1.0))
    val s2 = Array(("宝莱坞", 6.5), ("同性", 5.3), ("同性恋", 4.4), ("小人物", 4.0), ("18禁", 3.8), ("歌舞", 3.5), ("文艺", 2.3), ("人性", 2.3), ("演员", 2.1), ("亲情", 1.6), ("家庭", 1.5), ("现代", 1.4))

    val s3 = Array("宝莱坞", "同性", "同性恋", "小人物")

    val sk = s2.toMap
    val all = s3.map { x =>
      val st1 = sk.get(x)

      (x, (x, sk.get(x).mkString.toDouble))
    }
    all.toSeq.toDF().show()

    val sst = s2.map { x => x._1 -> x._2 }


    var ss1: Map[String, Double] = List().toMap
    var ss3 = Map("nihao" -> 3.0, "otehr" -> 11.1)
    var ss2 = Map("nihao" -> 2.0, "ttt" -> 100.1, "work" -> 4, "fire" -> 12.5)
    var ss4 = Map("work" -> 5.1, "fire" -> 7.5)
    var tt: Seq[(String, Double)] = null

    val dd = ss3.toList.union(ss2.toList).union(ss1.toList)

    // dd.foreach(println)
    val sds = dd.groupBy(_._1).map { x =>
      val key = x._1
      val ttv = x._2.map(_._2).mkString(",")
      (key, ttv.split(",").map(x => x.toDouble).sum)

    }.toList.sortBy(_._2).reverse
    val avgnum = (sds.map(_._2).max + sds.map(_._2).min) / 2
    println(sds.length)
    val sd2 = sds.take(math.ceil(sds.length / 2.0).toInt)
    //    val sdsfilter = sds.filter(_._2 >= avgnum)
    //    sdsfilter.foreach(println)
    val sd3 = sd2.map(_._1).mkString(",")
    println(sd3)


    //    dd.groupBy(_._1).foreach{x=>
    //      println(x._2.map(_._2).mkString(","))
    //    }


    val sstd = getunion(ss1, ss2, ss3, ss4)
    //sstd.foreach(println)


    val input = sc.textFile("D:\\data\\sample_lda_data.txt").map(line => line.split(" ").toSeq)

    val word2vec = new Word2Vec()
      .setInputCol("tokens")
      .setOutputCol("result")
    //
    //    val model = word2vec.fit(input)
    //
    //    val rrt = model.getVectors
    //
    //
    //    val synonyms = model.findSynonyms("1", 5)


    val data = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(6.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )

    val df = data.map(Tuple1.apply).toDF("features")
    df.show()

    // val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
    // println(s"Pearson correlation matrix:\n $coeff1")

    val te1 = Seq(("动画", 120), ("剧情", 240), ("武打", 0), ("科幻", 100))

    val te2 = Seq("动画", "剧情")

    //   te1.map{x=>
    //     te1.
    //   }
    val ss = te1.map { x =>
        if (!te2.contains(x._1)){
          x
        }else{
          null
        }
    }

  val rs =  ss match {case x:Seq[(String,Int)]=>x}
    rs.filter(x=>x.!=(null)).foreach(println)


  }

  def getunion(args: Map[String, AnyVal]*): Map[String, AnyVal] = {
    val ss = args.map { x =>
      x.toList
    }.reduce((x, y) => x.union(y))
    val sds = ss.groupBy(_._1).map { x =>
      val key = x._1
      val ttv = x._2.map(_._2).mkString(",")
      (key, ttv.split(",").map(x => x.toDouble).sum)
    }
    sds
  }


  //
  //    if (ss1.size >= ss2.size) {
  //      tt = ss1.map { x =>
  //        if (ss2.get(x._1).nonEmpty) {
  //          (x._1, ss1.get(x._1).mkString.toDouble + ss2.get(x._1).mkString.toDouble)
  //        } else {
  //          (x._1, ss1.get(x._1).mkString.toDouble)
  //        }
  //      }.toSeq
  //
  //    } else {
  //      tt = ss2.map { x =>
  //        if (ss1.get(x._1).nonEmpty) {
  //          (x._1, ss2.get(x._1).mkString.toDouble + ss1.get(x._1).mkString.toDouble)
  //        } else {
  //          (x._1, ss2.get(x._1).mkString.toDouble)
  //        }
  //      }.toSeq
  //    }
  //    tt.foreach(println)
  //
  //  }



}
