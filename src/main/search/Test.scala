package search

/**
  * @author zhaoming on 2018-04-09 15:27
  **/
object Test {
  def main(args: Array[String]): Unit = {
    val se=Seq((2,12,2),(4,5,6))
    val se2 =Seq((3,23,3))
   val s3 = se.union(se2)
    s3.foreach(println)

    val sk =Array("2","1","3")
    val sst = sk.mkString("[\"","\",\"","\"]")

    sk.map{x=>
    println("nihoh"+x)
      val s="23"
      s
    }.reduce((x,y)=>x+y)


    val stxx ="ni"

   val reg ="\\w".r
    println(stxx.matches("\\w"))

    println(sst)
  }
}
