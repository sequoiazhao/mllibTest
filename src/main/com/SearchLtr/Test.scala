package com.SearchLtr

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * @author zhaoming on 2018-06-15 14:43
  **/
object Test {

  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
    //      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    //    val sc = new SparkContext(conf)
    //    val MyContext = new HiveContext(sc)
    //
    //    val mdd = sc.textFile("D:\\dataframeData\\english3")
    //
    //    val sqlContext = SQLContext.getOrCreate(sc)
    //    import sqlContext.implicits._
    //    mdd.take(300).foreach(println)
    //
    //    val datardd = mdd.map { x =>
    //      val s1 = x.replace("[", "").replace("]", "").split(",")

    // (s1(0), s1(1), s1(2), s1(3), s1(4), s1(5), s1(6), s1(7), s1(8), s1(9), s1(10), s1(11))
    //
    //    val str = "12 fwwe 233242 fwefwe "
    //
    //    println(math.ceil(math.log(0 + 2)))
    //
    //
    //    val arr1 = Array(Array("[nihao,wohao]", "[wohao,haha]", "dajiahao"),Array("对对", "搓搓", "没有"))
    //
    //
    //val cc =   arr1.reduce((x,y)=>x.union(y)).map(x=>x.replaceAll("\\[|\\]",""))
    //
    //cc.foreach(println)

    //    val arr = Array("nihao","ni","woejfwoeifjew","fewfe","werw")
    //
    //    arr.sortBy(x=>x.length)
    //        .take(3)
    //      .foreach(println)

    val conf = new SparkConf().setAppName("LDATest").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/spark-warehouse/")
    val sc = new SparkContext(conf)
    //val MyContext = new HiveContext(sc)

    val mdd = sc.textFile("D:\\dataframeData\\sss1.txt")
    mdd.foreach(println)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val sst = mdd.map { x =>
      val s1 = x.split(" ")
      (s1(0), s1(1).split(","))
    }.toDF("id", "tag")

    sst.printSchema()
    sst.show()


    val ma = Seq(Map("name" -> "你好", "value" -> "10.0"), Map("name" -> "大家", "value" -> "20.0"), Map("name" -> "我们", "value" -> "30.0"))

    //println(ma.get("你好").mkString.toDouble)

    val mt = Seq("你好", "大家")

    //    val ans = mt.map { x =>
    //
    //      ma.updated(x, ma.get(x).mkString.toDouble / 2)
    //    }
    //
    //    val stt = mt.flatMap { x =>
    //      val ss = ma(x).toString.toDouble / 2.0
    //
    //      Map(x -> ss)
    //    }.toMap
    //    println(stt)


    //    val rtt= ma.map{ x =>
    //        if (mt.contains(x._1)) {
    //          (x._1 , x._2.toString.toDouble / 2)
    //        } else {
    //          x
    //      }
    //    }

    //    val rtt = ma.map{x=>
    //
    //       if(mt.contains(x.get("name").mkString){
    //
    //      }
    //    }
    ma.foreach(x => println(x.get("name").mkString))

    val tempd = ma.map { x =>
      if (mt.contains(x.get("name").mkString)) {
        Map("name" -> x.get("name").mkString, "value" -> (x.get("value").mkString.toDouble / 2.0).toString)
      } else {
        x
      }
    }

    tempd.foreach(println)



    println(System.currentTimeMillis() / 1000)

    // ans.foreach(println)
    val s = 1.0

    if (s.equals(1.0)) {
      println("ssss")
    }

  }

}
