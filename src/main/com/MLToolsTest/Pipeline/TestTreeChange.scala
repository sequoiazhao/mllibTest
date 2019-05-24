package com.MLToolsTest.Pipeline

import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
  * @author zhaoming on 2018-06-21 19:09
  **/
object TestTreeChange {
  def main(args: Array[String]): Unit = {
    val treestr =
      """RandomForest regressor with 10 trees
                
                  Tree 0:
                    If (feature 3 <= 3.321928)
                     If (feature 2 <= 3.0773122)
                      If (feature 8 <= 19.0)
                       Predict: -0.22145745232887645
                      Else (feature 8 > 19.0)
                       Predict: -0.1258417690540053
                     Else (feature 2 > 3.0773122)
                      If (feature 7 <= 72.70059)
                       Predict: 6.479900727323004E-4
                      Else (feature 7 > 72.70059)
                       Predict: -0.08202249896662053
                    Else (feature 3 > 3.321928)
                     If (feature 3 <= 7.5999126)
                      If (feature 7 <= 300.82217)
                       Predict: 0.13196649600961874
                      Else (feature 7 > 300.82217)
                       Predict: 0.5251246413492967
                     Else (feature 3 > 7.5999126)
                      If (feature 1 <= 0.5416666666666666)
                       Predict: 0.10710728184977467
                      Else (feature 1 > 0.5416666666666666)
                       Predict: 0.6060906238557671
                  Tree 1:
                    If (feature 2 <= 7.176713)
                     If (feature 1 <= 0.8125)
                      If (feature 3 <= 3.321928)
                       Predict: -0.017011405005760687
                      Else (feature 3 > 3.321928)
                       Predict: -0.19019550234214797
                     Else (feature 1 > 0.8125)
                      Predict: 2.542088613941175
                    Else (feature 2 > 7.176713)
                     If (feature 6 <= 1.0)
                      If (feature 3 <= 5.857981)
                       Predict: 0.12474237446325069
                      Else (feature 3 > 5.857981)
                       Predict: 0.39170186606552426
                     Else (feature 6 > 1.0)
                      If (feature 7 <= 229.80061)
                       Predict: -0.027926281091753404
                      Else (feature 7 > 229.80061)
                       Predict: 0.21578189299365064
                  Tree 2:
                    If (feature 2 <= 6.5661097)
                     If (feature 1 <= 0.6842105263157895)
                      If (feature 3 <= 3.321928)
                       Predict: -0.016380888308118367
                      Else (feature 3 > 3.321928)
                       Predict: -0.1473011097146559
                     Else (feature 1 > 0.6842105263157895)
                      If (feature 7 <= 167.87843)
                       Predict: -1.0553460162202202
                      Else (feature 7 > 167.87843)
                       Predict: -0.012938201226014487
                    Else (feature 2 > 6.5661097)
                     If (feature 7 <= 197.23137)
                      If (feature 3 <= 3.321928)
                       Predict: 0.14868045415401446
                      Else (feature 3 > 3.321928)
                       Predict: -0.026342267781998618
                     Else (feature 7 > 197.23137)
                      If (feature 7 <= 215.62454)
                       Predict: 1.0445391943056206
                      Else (feature 7 > 215.62454)
                       Predict: 0.14508213705703543
                  Tree 3:
                    If (feature 2 <= 5.88527)
                     If (feature 1 <= 0.6842105263157895)
                      If (feature 3 <= 3.321928)
                       Predict: -0.017845902524561665
                      Else (feature 3 > 3.321928)
                       Predict: -0.17941505579937864
                     Else (feature 1 > 0.6842105263157895)
                      If (feature 6 <= 1.0)
                       Predict: 0.07184766008578378
                      Else (feature 6 > 1.0)
                       Predict: -1.9530950215128036
                    Else (feature 2 > 5.88527)
                     If (feature 6 <= 1.0)
                      If (feature 3 <= 5.857981)
                       Predict: 0.06888057535977807
                      Else (feature 3 > 5.857981)
                       Predict: 0.30572194734467084
                     Else (feature 6 > 1.0)
                      If (feature 3 <= 3.321928)
                       Predict: 0.1271423540135556
                      Else (feature 3 > 3.321928)
                       Predict: -0.05563293890607136
                  Tree 4:
                    If (feature 2 <= 7.176713)
                     If (feature 1 <= 0.8125)
                      If (feature 4 <= 0.0)
                       Predict: -0.050770903782968536
                      Else (feature 4 > 0.0)
                       Predict: 0.024506190247153697
                     Else (feature 1 > 0.8125)
                      Predict: 1.990878348669579
                    Else (feature 2 > 7.176713)
                     If (feature 1 <= 0.7222222222222222)
                      If (feature 3 <= 5.857981)
                       Predict: 0.009932743328890374
                      Else (feature 3 > 5.857981)
                       Predict: 0.2374293082450342
                     Else (feature 1 > 0.7222222222222222)
                      If (feature 3 <= 7.5999126)
                       Predict: 0.24582511536443508
                      Else (feature 3 > 7.5999126)
                       Predict: -0.4686678187504114
                  Tree 5:
                    If (feature 7 <= 240.11435)
                     If (feature 6 <= 1.0)
                      If (feature 2 <= 5.625821)
                       Predict: -0.01943087715100151
                      Else (feature 2 > 5.625821)
                       Predict: 0.10245772309268415
                     Else (feature 6 > 1.0)
                      If (feature 1 <= 0.7647058823529411)
                       Predict: -0.07943906490688876
                      Else (feature 1 > 0.7647058823529411)
                       Predict: 0.2835727230986883
                    Else (feature 7 > 240.11435)
                     If (feature 3 <= 5.857981)
                      If (feature 7 <= 300.82217)
                       Predict: 0.08289053581229551
                      Else (feature 7 > 300.82217)
                       Predict: -0.2636180958457523
                     Else (feature 3 > 5.857981)
                      If (feature 1 <= 0.65)
                       Predict: 0.990348812200151
                      Else (feature 1 > 0.65)
                       Predict: -0.33902336981966574
                  Tree 6:
                    If (feature 3 <= 0.0)
                     If (feature 1 <= 0.3023255813953488)
                      If (feature 7 <= 79.25243)
                       Predict: -0.7176262935346044
                      Else (feature 7 > 79.25243)
                       Predict: 0.043064305781218636
                     Else (feature 1 > 0.3023255813953488)
                      If (feature 2 <= 4.130355)
                       Predict: 0.00533697967062476
                      Else (feature 2 > 4.130355)
                       Predict: -0.08562245862654605
                    Else (feature 3 > 0.0)
                     If (feature 7 <= 197.23137)
                      If (feature 7 <= 76.850845)
                       Predict: 0.027592404581360478
                      Else (feature 7 > 76.850845)
                       Predict: -0.08189027576692097
                     Else (feature 7 > 197.23137)
                      If (feature 3 <= 5.857981)
                       Predict: 0.06573655564387071
                      Else (feature 3 > 5.857981)
                       Predict: 0.2773825905826357
                  Tree 7:
                    If (feature 2 <= 5.3584714)
                     If (feature 1 <= 0.6842105263157895)
                      If (feature 1 <= 0.4482758620689655)
                       Predict: -0.050504833728161044
                      Else (feature 1 > 0.4482758620689655)
                       Predict: 0.03119899223405797
                     Else (feature 1 > 0.6842105263157895)
                      If (feature 7 <= 0.0)
                       Predict: -1.906517569025203
                      Else (feature 7 > 0.0)
                       Predict: 0.12882376355296662
                    Else (feature 2 > 5.3584714)
                     If (feature 3 <= 3.321928)
                      If (feature 3 <= 1.5849625)
                       Predict: -0.008333980756966142
                      Else (feature 3 > 1.5849625)
                       Predict: 0.20766610193079493
                     Else (feature 3 > 3.321928)
                      If (feature 6 <= 1.0)
                       Predict: 0.07258495155699865
                      Else (feature 6 > 1.0)
                       Predict: -0.07109331106653984
                  Tree 8:
                    If (feature 2 <= 9.294525)
                     If (feature 1 <= 0.7647058823529411)
                      If (feature 3 <= 3.321928)
                       Predict: 0.0048894727151880995
                      Else (feature 3 > 3.321928)
                       Predict: -0.08038574614412096
                     Else (feature 1 > 0.7647058823529411)
                      If (feature 2 <= 6.8308744)
                       Predict: 0.9653100161405215
                      Else (feature 2 > 6.8308744)
                       Predict: 0.15143017658660796
                    Else (feature 2 > 9.294525)
                     If (feature 1 <= 0.65)
                      If (feature 3 <= 5.857981)
                       Predict: -0.048226431638672164
                      Else (feature 3 > 5.857981)
                       Predict: 0.3374632044250847
                     Else (feature 1 > 0.65)
                      If (feature 3 <= 7.5999126)
                       Predict: 0.09879376524214979
                      Else (feature 3 > 7.5999126)
                       Predict: -0.3216061410074944
                  Tree 9:
                    If (feature 4 <= 1.0)
                     If (feature 1 <= 0.43333333333333335)
                      If (feature 3 <= 5.857981)
                       Predict: -0.06371883352181906
                      Else (feature 3 > 5.857981)
                       Predict: 1.7787220985499026
                     Else (feature 1 > 0.43333333333333335)
                      If (feature 2 <= 3.0773122)
                       Predict: 0.07981116202081591
                      Else (feature 2 > 3.0773122)
                       Predict: -0.00445636069772129
                    Else (feature 4 > 1.0)
                     If (feature 1 <= 0.8125)
                      If (feature 7 <= 300.82217)
                       Predict: 0.03754272372171544
                      Else (feature 7 > 300.82217)
                       Predict: -0.5928214735144876
                     Else (feature 1 > 0.8125)
                      Predict: 2.0950412251138797"""

    val sstarray = treestr.split("\n")
    //sstarray.foreach(println)
    //println(sstarray.length)
    val ss2 = treestr.split("\n").toIterator
    ss2.foreach(println)


    var index1 = 0
    var index2 = 0

    val stack = mutable.Stack[Int]()


    var split = 0
    var laststr = ""

    val treeXml = sstarray.map { x =>
      val numRegex ="""(-)?\d+(\.\d+)?""".r
      var str = ""

      if (x.contains("RandomForest")) {
        str = "## LambdaMART" + x.substring(x.length - 7, x.length - 6)
        str = str + '\n' + "<ensemble>"
      }


      if (x.contains("Tree")) {
        val starry = x.split("\\(")
        if (index1 == 0) {
          str = str + "\n" + "<tree id=\"" + (numRegex.findAllMatchIn(starry.apply(0)).toList.head.toString.toInt + 1) +
            //"\" weight=\"" + numRegex.findAllMatchIn(starry.apply(1)).toList.head +
            "\">" + "\n" +
            "<split>"
          index1 = index1 + 1
          stack.push(0)
        } else {
          for (ind <- 1 to split - 1) {
            str = str + "\n" + "</split>"
          }
          split = 0
          str = str + "\n" + "</tree>" + "\n\n" + "<tree id=\"" + (numRegex.findAllMatchIn(starry.apply(0)).toList.head.toString.toInt + 1) +
            //"\" weight=\"" + numRegex.findAllMatchIn(starry.apply(1)).toList.head +
            "\">" + "\n" +
            "<split>"

          println((numRegex.findAllMatchIn(starry.apply(0)).toList.head.toString.toInt + 1))
          stack.clear()
          stack.push(0)
          index2 = 0
        }

      }

      if (x.contains("If")) {
        val starry = x.split("<=")
        str = str + "<feature> " + numRegex.findAllMatchIn(starry.apply(0)).toList.head + " </feature>" + "\n" +
          "<threshold> " + numRegex.findAllMatchIn(starry.apply(1)).toList.head + " </threshold>" + "\n" +
          "<split pos=\"left\">"

        stack.push(1)
      }

      if (x.contains("Else")) {
        str = str + "<split pos=\"right\">"
        split = split + 1
        stack.push(2)
      }

      if (x.contains("Predict")) {
        str = str + "<output>" + numRegex.findAllMatchIn(x).toList.head + " </output>" + "\n" +
          "</split>"

        //判断是否是右叶子节点
        //同时判断是否右叶子节点，出栈到最后一个1
        //最后一个1是在下棵树开始时出的
        //每次出栈的数量怎么判断，尤其是大于3
        if (laststr.contains("left")) {
          println("left leaf")
        } else {
          println("right leaf")
          println(stack)
//          for (ind <- 1 to split) {
//            str = str + "</split>"
//          }
//          stack.pop()
//          stack.pop()
//          stack.pop()
          split = 0
          //出栈规则，先出两个换成一个split ,判断栈顶是1还是2，是2再出直到1为止
          while (stack.head!=2) {
           println(stack.head)
           if(stack.head==2){
             println("true")
           }


            str=str+"</split>"
          }
          stack.pop()
          stack.pop()
          stack.pop()

        }
        //        if(index2%2==1){
        //
        //          for (ind<-1 to split){
        //            str =str+"</split>"
        //          }
        //          split=0
        //          println(stack)
        //          stack.pop()
        //          stack.pop()
        //          stack.pop()
        //         // stack.pop()
        //
        //        }
        str = str + "num:" + split
        index2 = index2 + 1

      }
      laststr = str
      str
    }

    treeXml.foreach(println)

    var treeEndXml = ""
    for (ind <- 1 to split - 1) {
      treeEndXml = treeEndXml + "\n" + "</split>"
    }

    treeEndXml = treeEndXml + "</tree></ensemble>"

    println(treeEndXml)
    //    val ssss2 = "3.321928"
    //    val numRegex ="""\d+(\.\d+)?""".r
    //
    //    println(numRegex.findAllMatchIn(ssss2).toList.head)


  }
}
