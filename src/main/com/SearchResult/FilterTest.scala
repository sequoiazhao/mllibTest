package com.SearchResult

import breeze.linalg.DenseMatrix
import org.ansj.splitWord.analysis.DicAnalysis
import org.apache.spark.sql.functions._
import org.apache.lucene.search._

import scala.util.matching.Regex

/**
  * @author zhaoming on 2018-04-23 12:36
  **/
object FilterTest {
  def main(args: Array[String]): Unit = {
    val p_t=new Regex("[\u4e00-\u9fa5]+")

    val ss = p_t.findAllMatchIn("白雪公主")
    //println( ss.mkString)


    val testSentence = DicAnalysis.parse(ss.mkString)
      .toStringWithOutNature(",").split(",")


  testSentence.foreach(println)

    val t1 ="bx"

    val t2 ="bxc"

   println(t2.indexOf(t1)+1)

    println(editDist(t1,t2))

    NG

  }


  //编辑距离
  def editDist(s1:String, s2:String):Int ={
    val s1_length = s1.length+1
    val s2_length = s2.length+1

    val matrix = DenseMatrix.zeros[Int](s1_length, s2_length)
    for(i <- 1.until(s1_length)){
      matrix(i,0) = matrix(i-1, 0) + 1
    }

    for(j <- 1.until(s2_length)){
      matrix(0,j) = matrix(0, j-1) + 1
    }

    var cost = 0
    for(j <- 1.until(s2_length)){
      for(i <- 1.until(s1_length)){
        if(s1.charAt(i-1)==s2.charAt(j-1)){
          cost = 0
        }else{
          cost = 1
        }
        matrix(i,j)=math.min(math.min(matrix(i-1,j)+1,matrix(i,j-1)+1),matrix(i-1,j-1)+cost)
      }
    }
    matrix(s1_length-1,s2_length-1)
  }

}
