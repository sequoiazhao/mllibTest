package com.SearchResult

import com.github.stuxuhai.jpinyin.PinyinHelper
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import org.apache.lucene.search.spell.NGramDistance
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, UserDefinedFunction}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.util.matching.Regex

/**
  * @author zhaoming on 2018-04-19 15:48
  **/
object getSampleGrade {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    val MyContext = new HiveContext(sc)

    val mdd = sc.textFile("D:\\dataframeData\\englocal01")

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val datardd = mdd.map { x =>
      val s1 = x.replace("[", "").replace("]", "").split(",")
      (s1(0), s1(1), s1(2), s1(3), s1(4), s1(5), s1(6), s1(7), s1(8), s1(9), s1(10), s1(11))
    }

    val testData = Array("LH", "MH", "XC", "XCA", "SG", "DG", "BP", "DL", "ZH", "XCM", "BX")

    var allString = ""

    val sss = testData.map { x =>


      val title = x

      val SampleDataOriginal = datardd.toDF("score", "keyid", "rank", "mediaid", "lengthweight", "logplaytimes", "logsearchtimes", "new", "fee", "categoryweight", "srcsearchkey", "title")
        .filter(col("srcsearchkey").===(lit(title)))
        .select("title", "score", "mediaid", "logplaytimes", "logsearchtimes", "srcsearchkey")

      val colKeyScore = udfTitleScore(SampleDataOriginal.col("title").cast(StringType), SampleDataOriginal.col("srcsearchkey"))

      val SampleData = SampleDataOriginal.withColumn("keyscore", colKeyScore)
        .withColumn("allscore", colKeyScore.+(lit(SampleDataOriginal.col("score"))))
        .sort(col("allscore").desc)
      SampleData.show(30, false)


      val sampleRdd = SampleDataOriginal.rdd.toArray()
      sampleRdd

    }


  }

  val ng: NGramDistance = new NGramDistance()

  def funTitleScore(Title: String, SearchKey: String): Int = {
    val filterCh = new Regex("[\u4e00-\u9fa5]+")
    //过滤中文中的字符
    val title = filterCh.findAllMatchIn(Title).mkString
    if (title != "") {

      //判断匹配得分

      val titlePy = PinyinHelper.getShortPinyin(title).toUpperCase
      val posScore = math.ceil(ng.getDistance(SearchKey, titlePy) * 10).toInt


      //分词
      //println(title)
      //val wordArray = DicAnalysis.parse(title).toStringWithOutNature(",").split(",")


      //hanlp分词
      val wordArray = StandardTokenizer.segment(title).map(x => x.word.replaceAll(" ", "")).mkString(",").split(",")

      val result = wordArray.map { x =>
        //取首字母
        val wordPy = PinyinHelper.getShortPinyin(x).toUpperCase

        if (wordPy == SearchKey) {
          20 / title.length
        }
        else {
          0
        }

      }.sum


      result + posScore
    } else {
      0
    }
  }

  val udfTitleScore: UserDefinedFunction = udf(funTitleScore _)


}


//  def GetFirstC(chStr: String): String = {
//    val ch = Array("啊", "芭", "擦", "搭", "蛾", "发", "噶",
//      "哈", "击", "喀", "垃", "妈", "拿", "哦", "啪", "期", "然", "撒",
//      "塌", "挖", "昔", "压", "匝")
//
//    val cx = Array("A", "B", "C", "D", "E", "F", "G",
//      "H", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S",
//      "T", "W", "X", "Y", "Z")
//
//    val chk = ch.map { x =>
//      val sx = x.getBytes("GB2312")
//      sx(0).<<(8).&(0xff00) + sx(1).&(0xff)
//    }
//
//    val sch = chStr.toCharArray.map(_.toString).map { x =>
//      val sx = x.getBytes("GB2312")
//      sx(0).<<(8).&(0xff00) + sx(1).&(0xff)
//    }
//
//    val resultStr = sch.map { x =>
//      var result = ""
//      for (i <- 0 until chk.length - 1) {
//        if (x >= chk(i) && x < chk(i + 1)) {
//          result = cx(i)
//          result
//        }
//      }
//
//      if (result != "") {
//        result
//      } else {
//        "Z"
//      }
//    }.reduce((x, y) => x + y)
//    resultStr
//  }

