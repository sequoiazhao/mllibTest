package com.MLToolsTest.otherALS

import scala.collection.mutable

object SGD {
  val data =mutable.HashMap[Int,Int]()

  def getData():mutable.HashMap[Int,Int]={
    for (i <-1 to 50){
      data+=(i->(2*i))
    }
    data.foreach(println)
    data
  }

  var the :Double = 0
  var alpha :Double =0.05

  def sgd(x:Double,y:Double) = {
    the =the -alpha*(the*x-y)
  }

  def main(args: Array[String]): Unit = {
    val dataSource = getData()

    dataSource.foreach(myMap =>
    {
      sgd(myMap._1,myMap._2)
      println("the result :"+the)
    })
    println("the result :"+the)
  }


}
