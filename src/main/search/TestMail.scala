package search

import com.hisense.search.ltr.Common.TestSendEmail

/**
  * @author zhaoming on 2018-09-17 15:32
  **/
object TestMail {
  def main(args: Array[String]): Unit = {
    val s = false
    if (!s) {
      TestSendEmail.sendErrorMail("TestMail")
    }
  }
}
