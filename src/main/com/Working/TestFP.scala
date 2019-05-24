package com.Working

import com.Common.GetConf
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth}
import org.apache.spark.rdd.RDD


/**
  * @author zhaoming on 2018-08-02 10:12
  **/
object TestFP {

  def main(args: Array[String]): Unit = {
    val getConf = new GetConf()
    val sc = getConf.getConfSingle

    val data = sc.textFile("D:\\data\\fpdata.txt")


    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(','))


    val fpg = new FPGrowth()
      .setMinSupport(0.3)
      .setNumPartitions(10)
    val model = fpg.run(transactions)

    model.freqItemsets.collect().foreach{itemset=>
      println(itemset.items.mkString("[",",","]")+", "+itemset.freq)
    }

  val minConfidence = 0.8
//    model.generateAssociationRules(minConfidence).collect().foreach{rule=>
//      println(
//        rule.antecedent.mkString("[", ",", "]")
//        +"=>"+rule.consequent.mkString("[", ",", "]")
//        +", "+rule.confidence
//      )
//    }

    val sss =model.generateAssociationRules(minConfidence).collect()

    val ar = new AssociationRules()
      .setMinConfidence(0.8)
    val results = ar.run(model.freqItemsets)

    results.collect().foreach { rule =>
      println("[" + rule.antecedent.mkString(",")
        + "=>"
        + rule.consequent.mkString(",") + "]," + rule.confidence)
    }
  }



}
