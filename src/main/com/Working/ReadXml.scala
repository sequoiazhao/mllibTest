package com.Working

import com.Common.GetXml
import org.apache.spark.sql.functions._

/**
  * @author zhaoming on 2018-08-01 17:10
  **/
object ReadXml {

  //  filterMovieWord
  def main(args: Array[String]): Unit = {
    val str = GetXml.regexMap.get("filterMovieWord").head.replaceAll("\n| ", "").split("#")
    str.toList.foreach(println)

    println(math.log(1))

    val y = (2010 - 1950) / 68.0

    println(y)
    val s = y
    println(1 / (1 + math.exp(-s * 10 + 5)))


    val testArray = Array(100)

    testArray.map { x =>
      ((x - testArray.min + 1) / ((testArray.max - testArray.min).toDouble+1)) * 10
    }.foreach(println)

  }

}
