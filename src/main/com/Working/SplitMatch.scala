package com.Working

import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
  * @author zhaoming on 2018-08-23 12:55
  **/
object SplitMatch {

  def main(args: Array[String]): Unit = {
    val strA = Array("你好, 你好兄弟, 你好朋友, 你好爱人"
      ,"奔跑吧兄弟第3季, 奔跑吧兄弟第4季, 奔跑吧第1季, 奔跑吧第2季, 奔跑吧！兄弟, 奔跑嘉年华, 奔跑吧, 奔跑吧2, 奔跑吧兄弟, 奔跑吧兄弟  第二季, 奔跑吧兄弟 第2季, 奔跑吧兄弟 第3季, 奔跑吧兄弟Ⅱ, 奔跑吧兄弟Ⅲ, 奔跑吧兄弟Ⅳ, 奔跑吧兄弟第1季, 奔跑吧兄弟第2季")


    //把这个数组拆开成Map

    val result2 = strA.map { str :String=>

      val strArray = str.split(",")
      var index = 0
      val result = strArray.map { x =>

        val res = (x, strArray.filterNot(s => s == x))
        index = index + 1
        res
      }
      result
    }.reduce((x,y)=>x.union(y))



    result2.foreach{x=>
      println(x._1)
    x._2.foreach(print)
      println()
    }



  }



}
