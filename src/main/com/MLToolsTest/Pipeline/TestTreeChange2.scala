package com.MLToolsTest.Pipeline


import org.json4s.jackson.Json
import play.api.libs.json.JsValue

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import util.control.Breaks._

/**
  * @author zhaoming on 2018-06-26 9:09
  **/
object TestTreeChange2 {
  def main(args: Array[String]): Unit = {
    val treestr ="RandomForestregressorwith10  trees\n Tree0:\n If(feature3<=3.321928)\n If(feature2<=3.0773122)\n If(feature8<=19.0)\n Predict:-0.22145745232887645\n Else(feature8>19.0)\n Predict:-0.1258417690540053\n Else(feature2>3.0773122)\n If(feature7<=72.70059)\n Predict:6.479900727323004E-4\n Else(feature7>72.70059)\n Predict:-0.08202249896662053\n Else(feature3>3.321928)\n If(feature3<=7.5999126)\n If(feature7<=300.82217)\n Predict:0.13196649600961874\n Else(feature7>300.82217)\n Predict:0.5251246413492967\n Else(feature3>7.5999126)\n If(feature1<=0.5416666666666666)\n Predict:0.10710728184977467\n Else(feature1>0.5416666666666666)\n Predict:0.6060906238557671"


    //val sstarray = treestr.split("\n")
    //sstarray.foreach(println)
    //println(sstarray.length)
   // val ss2 = treestr.split("\n").toIterator
    //ss2.foreach(println)
    //parse(ss2)

   // val ss3 = treestr.split("\n").toList

   // val ss4 = mutable.Stack[String]()

    treeJson1(treestr)

//    ss3.map { x =>
//      ss4.push(x)
//    }
//    // ss4.foreach(println)
//
//    //parse2(ss3)
//    val block3 = new mutable.Stack[String]()
//    val ssx = parse3(ss4, block3)
//
//    println("here")
//    ssx.foreach(println)

  }

  def sum3(xs: List[String]): String = {
    if (xs.isEmpty) "0"
    else xs.head + sum3(xs.tail)
  }

  def parse3(lines: mutable.Stack[String], block3: mutable.Stack[String]): mutable.Stack[String] = {
    //    val block2 = new ListBuffer[String]

    if (lines.isEmpty) {
      println("0123")
    }
    else {
      // println("sssss" + parse3(lines) + "DDD")
      val st = lines.pop()
      parse3(lines, block3)
      //println(st)
      if (st.contains("If")) {
        block3.push("dddd")
        block3.push(st)
      }

      if (st.contains("Else")) {
        block3.push("xxxx")
        block3.push(st)
      }

      if (st.contains("Else")) {
        block3.push("ssss")
      }
      //block3.push("dfdssff")

      //  }
      //while (lines.nonEmpty){
      //      if(lines.head.contains("If")){
      ////        val ss4 = lines.pop()
      ////        println(ss4+"dddd"+"s")
      //        //block3.push(ss4)
      //        lines.pop()
      //        println("sdfsss"+parse3(lines)+"fwfwe")
      //      }else{
      //        lines.pop()
      //      }
    }
    block3
  }

  def parse2(lines: List[String]): Unit = {
    val block2 = new ListBuffer[String]

    var line = lines
    while (line.nonEmpty) {
      // println(line.head)
      if (line.head.contains("If")) {
        line = line.drop(1)
        block2.append("sss" + parse2(line) + "cccc")
      } else {
        line = line.drop(1)
        block2.append("aaa" + parse2(line) + "cccc")
      }

    }
    block2.foreach(println)
  }

  def parse(lines: Iterator[String]): Unit = {
    val block = mutable.Stack[String]()
    //lines.foreach(println)
    println(lines.indexOf(2))
    val ssline = lines.toSeq
    println(ssline.take(2))
    //    while (lines.hasNext){
    //      println(lines.take(0))
    //      if (lines.take(0).toString.startsWith("If")){
    //        println(lines.take(0))
    //        lines.drop(0)
    //        block.push("sss"+parse(lines)+"ccc")
    //        }
    //      lines.drop(0)
    //    }
  }


  def getStatmentType(x: String): (String, String) = {
    val ifPattern = "If+".r
    val ifelsePattern = "Else+".r
    var t = ifPattern.findFirstIn(x.toString)
    if (t != None) {
      ("If", (x.toString).replace("If", ""))
    } else {
      var ts = ifelsePattern.findFirstIn(x.toString)
      if (ts != None) ("Else", (x.toString).replace("Else", ""))
      else ("None", (x.toString).replace("(", "").replace(")", ""))
    }
  }

  def delete[A](test: List[A])(i: Int) = test.take(i) ++ test.drop((i + 1))


  def BuildJson(tree: List[String]): List[Map[String, Any]] = {
    var block: List[Map[String, Any]] = List()
    var lines: List[String] = tree
   // tree.foreach(println)
   breakable {
      while (lines.length > 0) {
        //println("here")
        var (cond, name) = getStatmentType(lines(0))
        println("initial" + cond)
        if (cond == "If") {
          println("if" + cond)
          // lines = lines.tail
          lines = delete(lines)(0)
          println(name)
          block = block :+ Map("if-name" -> name, "children" -> BuildJson(lines))
          println("After pop Else State" + lines(0))
          val (p_cond, p_name) = getStatmentType(lines(0))
          // println(p_cond + " = "+ p_name+ "\n")
          cond = p_cond
          name = p_name
          println(cond + " after=" + name + "\n")
          if (cond == "Else") {
            println("else" + cond)
            lines = lines.tail
            block = block :+ Map("else-name" -> name, "children" -> BuildJson(lines))
          }
        } else if (cond == "None") {
          println(cond + "NONE")
          lines = delete(lines)(0)
          block = block :+ Map("predict" -> name)
        } else {
          println("Finaly Break")
          println("While loop--" + lines)
          break()

        }
      }
    }
    block
  }

  def treeJson1(str: String): Unit = {
   // val str = "If (feature 0 in {1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,10.0,11.0,12.0,13.0})\n   If (feature 0 in {6.0})\n      Predict: 17.0\n    Else (feature 0 not in {6.0})\n      Predict: 6.0\n  Else (feature 0 not in {1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,10.0,11.0,12.0,13.0})\n   Predict: 20.0"
    val x = str.replace(" ", "")
    val xs = x.split("\n").toList
    var js = BuildJson(xs)
    //println(MapReader.mapToJson(js))
   // Json.toJson("")
    js.foreach(println)

  }

}
