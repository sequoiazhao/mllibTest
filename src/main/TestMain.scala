import java.io.File

/**
  * @author zhaoming on 2017-12-18 13:43
  **/
object TestMain {
  def main(args: Array[String]): Unit = {
    println(System.getProperty("user.dir"))

    val directory = new File("")

    println(directory.getCanonicalPath)
    println(directory.getAbsolutePath)
  }


}
