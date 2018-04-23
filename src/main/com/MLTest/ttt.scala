package com.MLTest

import org.apache.spark.sql.functions._

/**
  * @author zhaoming on 2018-03-21 13:37
  **/
object ttt {

  def main(args: Array[String]): Unit = {
    val str ="{ddd},{ccc},{eee},{sss}"
    val tt = str.split(",").zipWithIndex
    tt.foreach(println)
    tt
  }

}
