package Working

import java.util.Vector

import java.util.Random

/**
  * @author zhaoming on 2018-09-28 10:52
  **/
object test928 {

  def main(args: Array[String]): Unit = {
    var rand = new Random(53)

    // var w =Vector(nums)

    val str = "(nihao,12.1,22.3),(nihao,22.1,1.2),(nihao,11,1)"

    val str2 = "nihao"
    println(str.contains(str2))



    val arraystr = str.split("\\),\\(")
      .map{line=>
        val temp = line.replace("""(""", "").replace(""")""", "")
        val arrayValue = temp.split(",")
        (arrayValue(0)
          ,arrayValue(1).toDouble
        ,arrayValue(2).toDouble)

      }.toList

    arraystr.foreach(println)

   val ttt =  arraystr.groupBy(_._1).map{value=>
      (value._1
        , value._2.map(_._2).max
        , value._2.map(_._3).sum).toString
    }

    ttt.foreach(println)

    val sss=Seq("0")

    val yy = sss(0)



  }

}
