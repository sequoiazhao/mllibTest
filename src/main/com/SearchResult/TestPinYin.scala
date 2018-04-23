package com.SearchResult

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
        (-sx(0)).<<(8).&(0xff00)+sx(0).&(0xff)

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
