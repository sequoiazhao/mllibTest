package com.SearchResult

import java.io.{BufferedInputStream, FileInputStream}
import java.util.Properties

import com.github.stuxuhai.jpinyin.PinyinHelper

/**
  * @author zhaoming on 2018-04-20 8:52
  **/
object TestPinYin {

  def main(args: Array[String]): Unit = {

    val str = "奔跑吧月日热血回归"

    val test = GetFirstC(str)
    println(test.toLowerCase())

    val test2 = PinyinHelper.getShortPinyin(str)
    println(test2)

    val properties = new Properties()

    //val in = new BufferedInputStream(new FileInputStream("/resources/config.properties"))
    val in = TestPinYin.getClass.getClassLoader.getResourceAsStream("config.properties")
    //val path = Thread.currentThread().getContextClassLoader.getResource("/config.properties").getPath
    //properties.load(new FileInputStream(path))
    properties.load(in)

    // properties.load(in)
    val data = properties.getProperty("Databasename")
    val data2 = properties.getProperty("tableName")

    println(data)
    println(s"ss $data2")

    val ss: Seq[String] = Seq()
    val bb: Seq[String] = Seq()

    println(ss.nonEmpty)

    val arrx = Array("(12,22,3),(22,33,1),(12,22,6),(22,35,2)","(12,22,3),(22,33,1),(12,22,6),(22,35,2)")

    println(arrx.mkString)

    val testStr = "[(12,22,3),(22,33,1),(12,22,6),(22,35,2)]"

    val art = testStr.split("\\),\\(")
      .map { x =>
        val temp = x.replace("""[(""", "").replace(""")]""", "")
        val temp2 = temp.split(",")
        (temp2(0), temp2(1).toDouble, temp2(2).toDouble)
      }.toList

    val temp3 = art.groupBy(_._1).map { x =>
      (x._1, x._2.map(_._2).max, x._2.map(_._3).sum)
    }


    temp3.foreach(println)

    //
    //    val art3 =  art2.groupBy(_._1).map{x=>
    //      val value = x._2.map(_._3).mkString(",")
    //      (x._1,value.split(",").map(x=>x.toDouble).sum)
    //
    //    }
    // art3.foreach(println)
  }


  def GetFirstC(chStr: String): String = {
    val ch = Array("啊", "芭", "擦", "搭", "蛾", "发", "噶",
      "哈", "击", "喀", "垃", "妈", "拿", "哦", "啪", "期", "然", "撒",
      "塌", "挖", "昔", "压", "匝")

    val cx = Array("A", "B", "C", "D", "E", "F", "G",
      "H", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S",
      "T", "W", "X", "Y", "Z")

    val chk = ch.map { x =>
      val sx = x.getBytes("GB2312")
      sx(0).<<(8).&(0xff00) + sx(1).&(0xff)
    }

    val sch = chStr.toCharArray.map(_.toString).map { x =>
      val sx = x.getBytes("GB2312")
      if (sx.length > 1) {
        sx(0).<<(8).&(0xff00) + sx(1).&(0xff)
      } else {
        (-sx(0)).<<(8).&(0xff00) + sx(0).&(0xff)

      }
    }

    val resultStr = sch.map { x =>
      var result = ""
      for (i <- 0 until chk.length - 1) {
        if (x >= chk(i) && x < chk(i + 1)) {
          result = cx(i)
          result
        }
      }

      if (result != "") {
        result
      } else {
        "Z"
      }
    }.reduce((x, y) => x + y)
    resultStr



  }

}
