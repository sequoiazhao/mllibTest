package com.Working

/**
  * @author zhaoming on 2018-07-30 16:32
  **/
object Test20180730 {

  def main(args: Array[String]): Unit = {
    val tagStr = "爱情的"
    val tagTemp = "其他"
    val tagArray = List("爱情剧", "科幻片", "喜剧")
    println(tagArray.head)
    sss(tagStr, tagTemp, tagArray)


    val mapTest =Array(Map("惊悚" -> 74.49491783976555), Map("惊悚"-> 68.20776416361332), Map("惊悚" -> 68.09356918931007), Map("恐怖" -> 67.93392059206963), Map("惊悚" -> 67.3590037226677), Map("恐怖" -> 66.29442485421896))


    mapTest.groupBy(_.keys).foreach{x=>


    println(x._2.flatMap(_.keys).head, x._2.flatMap(_.values).sum)



    }

    val ss2 = Seq(("恐怖",20),("惊悚",21))


    val ss3 =List("恐怖","鬼","你好")
    val ss5 = List("恐怖","激情","鬼")
    println(ss3.intersect(ss5).size)

  }

  def sss(tagStr: String, tagTeamp: String, tagArray: List[String]): Int = {
    val temp = tagArray.map { x =>
      x.replaceAll("([\u4e00-\u9fa5][\u4e00-\u9fa5])剧", "$1")
        .replaceAll("([\u4e00-\u9fa5][\u4e00-\u9fa5])片", "$1")
    }
    List(tagStr, tagTeamp).intersect(temp).size
  }
}
