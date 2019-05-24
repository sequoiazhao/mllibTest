
import java.io.File

/**
  * @author zhaoming on 2018-11-01 15:09
  **/
object ScalaTest {
  def main(args: Array[String]): Unit = {
    //    val breakException = new RuntimeException("break exception")
    //
    //    def breakable(op : => Unit){
    //      try{
    //        op
    //      }catch {case _ =>}
    //    }
    //
    //    def break = throw  breakException
    //
    //    def install ={
    //      val env =System.getenv("SCALA_HOME")
    //      if(env == null ) break
    //      println("found nothing")
    //    }
    //    install

    val files = new File(".").listFiles
    //files.foreach(println)

    for (
      file <- files;
      fileName = file.getName
      if fileName.endsWith(".log")
    ) println(file)

    val aList = List(1, 2, 3)

    val bList = List(4, 5, 6)

    for {a <- aList; b <- bList} println(a + b)

   val res =  for {a <- aList; b<-bList} yield  a+b
    res.foreach(println)
ordinal(2)

  }

  def ordinal(num:Int)= num match {
    case 1=>println("lst")
    case 2=>println("ss2")
  }
}
