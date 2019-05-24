package com.SearchResult

import breeze.linalg.DenseMatrix
import com.hankcs.hanlp.dictionary.{CoreSynonymDictionary, CustomDictionary}
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import org.ansj.splitWord.analysis.DicAnalysis
import org.apache.lucene.search.spell._

import scala.collection.JavaConversions._
import scala.util.matching.Regex

/**
  * @author zhaoming on 2018-04-23 12:36
  **/
object FilterTest {
  def main(args: Array[String]): Unit = {
    val p_t = new Regex("[\u4e00-\u9fa5]+")

    val ss = p_t.findAllMatchIn("贝乐虎儿歌")
    //println( ss.mkString)


    val testSentence = DicAnalysis.parse(ss.mkString)
      .toStringWithOutNature(",").split(",")


    //testSentence.foreach(println)

        val t1 = "2016"

        val t2 = "2015东方卫视跨年盛典"

       // println(t2.indexOf(t1) + 1)

        //println(editDist(t1, t2))

    val lengthx = Math.abs(t1.length-t2.length)

    println(lengthx*editDist(t1, t2))

    //    val ng: NGramDistance = new NGramDistance()
    //    println(ng.getDistance(t1, t2))


    //测试分词器hanlp
    val sentense = "41,【 日  期 】19960104 【 版%%&%#*  号 】gergergeer1 【 标  题 】合日期版号《》巢芜geger高速公路wegeer巢芜56674段_,.;'~~!@#$%竣工 【 作  者 】彭建中 【 正  文 】     安徽合（肥）巢（湖）芜（湖）高速公路巢芜段日前竣工通车并投入营运。合巢芜 高速公路是国家规划的京福综合运输网的重要干线路段，是交通部确定１９９５年建成 的全国１０条重点公路之一。该条高速公路正线长８８公里。（彭建中）"
    //val sentense = "可不"
    // val sentense ="美好生活"
    //    CustomDictionary.add("日  期")
    CustomDictionary.add("版号")
    //    CustomDictionary.add("标  题")
    //    CustomDictionary.add("作  者")
    //    CustomDictionary.add("正  文")
    //    val s = CoreSynonymDictionary.similarity("你好","很好")
    //    println(s)


    val baseExpr =
      """[^\u4e00-\u9fa5]""".r
    val sss = baseExpr.replaceAllIn(sentense, "")
    println("0000:" + sss)

    val list = StandardTokenizer.segment(sss)
    println(list.toArray.mkString(","))

    CoreStopWordDictionary.apply(list)

    val strt = list.map { x =>
      if (x.nature.toString != "ad"
        && x.nature.toString != "v"
        && x.nature.toString != "d"
        && x.nature.toString != "s") {
        x.word.replaceAll(" ", "") + x.nature
      } else {
        ""
      }
    }
    println(strt.filter(x => x.length > 0).mkString(","))





  }


  //编辑距离
  def editDist(s1: String, s2: String): Int = {
    val s1_length = s1.length + 1
    val s2_length = s2.length + 1

    val matrix = DenseMatrix.zeros[Int](s1_length, s2_length)
    for (i <- 1.until(s1_length)) {
      matrix(i, 0) = matrix(i - 1, 0) + 1
    }

    for (j <- 1.until(s2_length)) {
      matrix(0, j) = matrix(0, j - 1) + 1
    }

    var cost = 0
    for (j <- 1.until(s2_length)) {
      for (i <- 1.until(s1_length)) {
        if (s1.charAt(i - 1) == s2.charAt(j - 1)) {
          cost = 0
        } else {
          cost = 1
        }
        matrix(i, j) = math.min(math.min(matrix(i - 1, j) + 1, matrix(i, j - 1) + 1), matrix(i - 1, j - 1) + cost)
      }
    }
    matrix(s1_length - 1, s2_length - 1)
  }

}
