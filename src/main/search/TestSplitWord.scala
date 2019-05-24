package search

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.DicAnalysis
import org.apache.spark.sql.functions._

/**
  * @author zhaoming on 2018-05-30 10:15
  **/
object TestSplitWord {

  val filter = new StopRecognition()
  filter.insertStopRegexes("[\\u4E00-\\u9FA5]")
  filter.insertStopRegexes("[\\x00-\\x1f]")
  filter.insertStopRegexes("\\d+(\\.\\d+)?(\\/\\d+)?")
  filter.insertStopRegexes("[` ~@#$^&*()=?|{}':;',\\[\\].\"<>《》/~@#￥……&*（）——|{}【】•.‘；：”“'。，、%+_-]!")
  filter.insertStopNatures("w")
  filter.insertStopNatures("m")
  filter.insertStopNatures("d")
  filter.insertStopNatures("r")
  filter.insertStopNatures("p")
  filter.insertStopNatures("c")
  filter.insertStopNatures("d")
  filter.insertStopNatures("nr")
  filter.insertStopNatures("ul")
  filter.insertStopNatures("uj")
  filter.insertStopNatures("l")
  filter.insertStopNatures("b")


  def main(args: Array[String]): Unit = {


    val str ="""{"media_id":123456, "summary":"欢迎使用ansj_seg,(ansj中文分词)在这里如果你遇到什么问题都可以联系我.我一定尽我所能.帮助大家.ansj_seg更快,更准,更自由!","type_name":"电影","title":"1","tag_name_array":[{"tag_name_array1":"2"}],"tag_new_name_array":[{"tag_new_name_array1":"2"}],"category_name_array":[{"category_name_array1":"2"}],"category_new_name_array":[{"category_new_name_array1":"2"}]}"""
    val result = DicAnalysis.parse(str).recognition(filter)
      .toStringWithOutNature

    println(result)

  }
}
