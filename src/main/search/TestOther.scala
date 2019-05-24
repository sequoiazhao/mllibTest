package search

/**
  * @author zhaoming on 2018-04-09 15:27
  **/
object TestOther {
  def main(args: Array[String]): Unit = {
    val se = Seq((2, 12, 2), (4, 5, 6))
    val se2 = Seq((3, 23, 3))

    val s3 = se.union(se2)
    s3.foreach(println)

    val sk = Array("2", "1", "3")
    val sst = sk.mkString("[\"", "\",\"", "\"]")

    val sk2 = sk.zipWithIndex
    sk2.foreach(println)

    sk.map { x =>
      println("nihoh" + x)
      val s = "23"
      s
    }.reduce((x, y) => x + y)


    val stxx = "ni"

    val reg = "\\w".r
    println(stxx.matches("\\w"))

    println(sst)

    val sss = Seq((10.2, 2), (11.2, 1), (2.34, 4))

    // println(sss.max)

    val te1 = sss.max

    val s1 = sss.filter(_.ne(te1)).max
    println(s1._1)

    val m1 = Map("动作" -> 12.0, "友情" -> 22.0, "喜剧" -> 11.3, "当代" -> 45.2, "悬疑" -> 21.1, "搞笑" -> 22.2, "烧脑" -> 1.0, "犯罪" -> 2.1, "逃亡" -> 9.0)

    val m2 = Map("动作" -> 2.0, "友情" -> 72.0, "悬疑" -> 11.1, "搞笑" -> 2.2, "烧脑" -> 11.0)

    val m3 = Map("当代" -> 95.2, "悬疑" -> 11.1, "搞笑" -> 32.2, "犯罪" -> 12.1, "逃亡" -> 29.0)



    println(math.ceil(12.5),math.ceil(12.2),math.floor(12.5),math.floor(12.2))


    //     val sss2 = sss.map{ x=>
    //       var ss:(Double,Int)=null
    //       if(x._2!=te1._2){
    //         ss =x
    //       }
    //         ss
    //     }


    // println(ttt.max)

    val ss1 = "ddddddds"
    println(ss1.mkString("\""))




  }
}
