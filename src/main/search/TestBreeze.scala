package search

import java.text.SimpleDateFormat
import java.util.Locale

import breeze.linalg._

/**
  * @author zhaoming on 2018-05-22 19:05
  **/
object TestBreeze {
  def main(args: Array[String]): Unit = {

    val x = DenseVector.zeros[Double](5)
    println(x(2))

    DenseVector(1, 2, 3).foreach(println)
    x(2) = 3.0
    println(x)
    x(1 to 2) := DenseVector(0.1, 0.2)

    println(x)

    val m = DenseMatrix.zeros[Int](4, 3)
    m(2, 2) = 10
    println(m)

    m(3, ::) := DenseVector(1, 2, 3).t

    println(m(::, 2))
    println(m(1, ::))
    println(m)


    val dm = DenseMatrix((1.0, 2.0, 3.0),
      (4.0, 5.0, 6.0))

    //array  to DenseMatrix and back
    val db = Array((1.0, 2.0, 2.1), (2.0, 3.0, 0.0), (4.5, 5.6, 7.2))



    val dm2 = DenseMatrix(db: _*)

    val dm4 = lowerTriangular(dm2)

    println(dm2)

    println(dm4)

    dm2.toArray.foreach(println)

    val dm3 =new  DenseMatrix(1,2,Array(1.0,2.0))
//
    println(dm3)


    //array to DenseVector

    val dv = DenseVector(Array(1.0,2.0,3.0))

    val a2 = Array(1.0,2.0,3.0)

    val dv2 = DenseVector(a2)

    println(dv)

    println(dv2)



    val  sss = List(("你好",12.0),("我好",11.0),("你好",22.0),("我好",8.0))

//    //change map to list and union all list
//    val temp = left.toList.union(right.toList)
//
//    //solve the problem of type conflict by changing double to string split
//    //{爱情=0.2,剧情=0.3} 变成 {"name":"爱情","value":0.2},{"name":"剧情","value":0.3}
//    val resultFilter = temp.groupBy(_._1).map { x =>
//      val value = x._2.map(_._2).mkString(",")
//      val valueDouble = value.split(",").map(x => x.toDouble).sum
//      (x._1, valueDouble)
//    }.toList.sortBy(_._2).reverse

    val result = sss.groupBy(_._1).map(x=>(x._1,x._2.map(_._2).sum))

    result.foreach(println)


    //println(dm)
    //println(db)
    //    val ss =List("sss","ccc","ttt")
    //
    //    val tt = List("ttt")
    //
    //    ss.intersect(tt).foreach(x=>println(ss.indexOf(x)))
    //
    //    val stt = ss.zipWithIndex
    //
    //    stt.foreach(println)
    //
    //
        val loc = new Locale("en")
        val sss1 ="2015-09-17 15:15:21"
        val sss2 ="2017-09-17 15:15:21"
        val sss3 ="1970-09-17 15:15:21"

        println("dddd")
        println(sss3.length)

        val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",loc)

        val dt = fm.parse(sss1)

        val dt2 = fm.parse(sss2)

        val dt3 = fm.parse(sss3)

        println(dt.getTime/100000000000.0)
        println(dt2.getTime/1000000000000.0)
        println(dt3.getTime/100000000000.0)
    //
    ////    val st1 =0.6
    ////
    //    println(math.log(0)+1.73)
//    1000000000000
//    100000000000

    //
    //
    //    val arrayDou = Array(10.0,1.1,5.7,8)
    //
    //    val sumA = arrayDou.max+arrayDou.min
    //    val a2 = arrayDou.map(_/sumA)
    //    a2.foreach(println)
  }

}
